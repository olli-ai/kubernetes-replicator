package replicate

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

type replicatorActions interface {
	getMeta(object interface{}) *metav1.ObjectMeta
	update(r *replicatorProps, object interface{}, sourceObject interface{}) error
	clear(r *replicatorProps, object interface{}) error
	install(r *replicatorProps, meta *metav1.ObjectMeta, sourceObject interface{}, dataObject interface{}) error
	delete(r *replicatorProps, meta interface{}) error
}

type objectReplicator struct {
	replicatorProps
	replicatorActions
}

func (r *objectReplicator) Synced() bool {
	return r.namespaceController.HasSynced() && r.objectController.HasSynced()
}

func (r *objectReplicator) Start() {
	log.Printf("running %s object controller", r.Name)
	go r.namespaceController.Run(wait.NeverStop)
	go r.objectController.Run(wait.NeverStop)
}

func (r *objectReplicator) NamespaceAdded(object interface{}) {
	namespace := object.(*v1.Namespace)
	log.Printf("new namespace %s", namespace.Name)
	// find all the objects which want to replicate to that namespace
	todo := map[string]bool{}

	for source, watched := range r.watchedTargets {
		for _, ns := range watched {
			if namespace.Name == strings.SplitN(ns, "/", 2)[0] {
				todo[source] = true
				break
			}
		}
	}

	for source, patterns := range r.watchedPatterns {
		if todo[source] {
			continue
		}

		for _, p := range patterns {
			if p.MatchNamespace(namespace.Name) != "" {
				todo[source] = true
				break
			}
		}
	}
	// get all sources and let them replicate
	for source := range todo {
		if sourceObject, exists, err := r.objectStore.GetByKey(source); err != nil {
			log.Printf("could not get %s %s: %s", r.Name, source, err)
		// it should not happen, but maybe `ObjectDeleted` hasn't been called yet
		// just clean watched targets to avoid this to happen again
		} else if !exists {
			log.Printf("%s %s not found", r.Name, source)
			delete(r.watchedTargets, source)
			delete(r.watchedPatterns, source)
		// let the source replicate
		} else {
			log.Printf("%s %s is watching namespace %s", r.Name, source, namespace.Name)
			r.replicateToNamespace(sourceObject, namespace.Name)
		}
	}
}

func (r *objectReplicator) replicateToNamespace(object interface{}, namespace string) {
	meta := r.getMeta(object)
	key := fmt.Sprintf("%s/%s", meta.Namespace, meta.Name)
	// those annotations have priority
	if _, ok := meta.Annotations[ReplicatedByAnnotation]; ok {
		return
	}
	// get all targets
	targets, targetPatterns, err := r.getReplicationTargets(meta)
	if err != nil {
		log.Printf("could not parse %s %s: %s", r.Name, key, err)
		return
	}
	// find the ones matching with the namespace
	existingTargets := map[string]bool{}

	for _, target := range targets {
		if namespace == strings.SplitN(target, "/", 2)[0] {
			existingTargets[target] = true
		}
	}

	for _, pattern := range targetPatterns {
		if target := pattern.MatchNamespace(namespace); target != "" {
			existingTargets[target] = true
		}
	}
	// cannot target itself
	delete(existingTargets, key)
	if len(existingTargets) == 0 {
		return
	}
	// get the current targets in order to update the slice
	currentTargets, ok := r.targetsTo[key]
	if !ok {
		currentTargets = []string{}
	}
	// install all the new targets
	for target := range existingTargets {
		log.Printf("%s %s is replicated to %s", r.Name, key, target)
		currentTargets = append(currentTargets, target)
		r.installObject(target, nil, object)
	}
	// update the current targets
	r.targetsTo[key] = currentTargets
	// no need to update watched namespaces nor pattern namespaces
	// because if we are here, it means they already match this namespace
}

func (r *objectReplicator) ObjectAdded(object interface{}) {
	meta := r.getMeta(object)
	key := fmt.Sprintf("%s/%s", meta.Namespace, meta.Name)
	// get replication targets
	targets, targetPatterns, err := r.getReplicationTargets(meta)
	if err != nil {
		log.Printf("could not parse %s %s: %s", r.Name, key, err)
		return
	}
	// if it was already replicated to some targets
	// check that the annotations still permit it
	if oldTargets, ok := r.targetsTo[key]; ok {
		log.Printf("source %s %s changed", r.Name, key)

		sort.Strings(oldTargets)
		previous := ""
Targets:
		for _, target := range oldTargets {
			if target == previous {
				continue Targets
			}
			previous = target

			for _, t := range targets {
				if t == target {
					continue Targets
				}
			}
			for _, p := range targetPatterns {
				if p.MatchString(target) {
					continue Targets
				}
			}
			// apparently this target is not valid anymore
			log.Printf("annotation of source %s %s changed: deleting target %s",
				r.Name, key, target)
			r.deleteObject(target, object)
		}
	}
	// clean all thos fields, they will be refilled further anyway
	delete(r.targetsTo, key)
	delete(r.watchedTargets, key)
	delete(r.watchedPatterns, key)
	// check for object having dependencies, and update them
	if replicas, ok := r.targetsFrom[key]; ok {
		log.Printf("%s %s has %d dependents", r.Name, key, len(replicas))
		r.updateDependents(object, replicas)
	}
	// this object was replicated by another, update it
	if val, ok := meta.Annotations[ReplicatedByAnnotation]; ok {
		log.Printf("%s %s is replicated by %s", r.Name, key, val)
		sourceObject, exists, err := r.objectStore.GetByKey(val)

		if err != nil {
			log.Printf("could not get %s %s: %s", r.Name, val, err)
			return
		// the source has been deleted, so should this object be
		} else if !exists {
			log.Printf("source %s %s deleted: deleting target %s", r.Name, val, key)

		} else if ok, err := r.isReplicatedTo(r.getMeta(sourceObject), meta); err != nil {
			log.Printf("could not parse %s %s: %s", r.Name, val, err)
			return
		// the source annotations have changed, this replication is deleted
		} else if !ok {
			log.Printf("source %s %s is not replicated to %s: deleting target", r.Name, val, key)
			exists = false
		}
		// no source, delete it
		if !exists {
			r.doDeleteObject(object)
			return
		// source is here, install it
		} else if err := r.installObject("", object, sourceObject); err != nil {
			return
		// get it back after edit
		} else if obj, m, err := r.objectFromStore(key); err != nil {
			log.Printf("could not get %s %s: %s", r.Name, key, err)
			return
		// continue
		} else {
			object = obj
			meta = m
			targets = nil
			targetPatterns = nil
		}
	}
	// this object is replicated to other locations
	if targets != nil || targetPatterns != nil {
		existsNamespaces := map[string]bool{} // a cache to remember the done lookups
		existingTargets := []string{} // the slice of all the target this object should replicate to

		for _, t := range(targets) {
			ns := strings.SplitN(t, "/", 2)[0]
			var exists, ok bool
			var err error
			// already in cache
			if exists, ok = existsNamespaces[ns]; ok {
			// get it
			} else if _, exists, err = r.namespaceStore.GetByKey(ns); err == nil {
				existsNamespaces[ns] = exists
			}

			if err != nil {
				log.Printf("could not get namespace %s: %s", ns, err)
			} else if exists {
				existingTargets = append(existingTargets, t)
			} else {
				log.Printf("replication of %s %s to %s cancelled: no namespace %s",
					r.Name, key, t, ns)
			}
		}

		if len(targetPatterns) > 0 {
			namespaces := r.namespaceStore.ListKeys()
			// cache all existing targets
			seen := map[string]bool{key: true}
			for _, t := range(existingTargets) {
				seen[t] = true
			}
			// find which new targets match the patterns
			for _, p := range targetPatterns {
				for _, t := range p.Targets(namespaces) {
					if !seen[t] {
						seen[t] = true
						existingTargets = append(existingTargets, t)
					}
				}
			}
		}
		// save all those info
		if len(targets) > 0 {
			r.watchedTargets[key] = targets
		}

		if len(targetPatterns) > 0 {
			r.watchedPatterns[key] = targetPatterns
		}

		if len(existingTargets) > 0 {
			r.targetsTo[key] = existingTargets
			// create all targets
			for _, t := range(existingTargets) {
				log.Printf("%s %s is replicated to %s", r.Name, key, t)
				r.installObject(t, nil, object)
			}
		}
		// in this case, replicate-from annoation only refers to the target
		// so should stop now
		return
	}
	// this object is replicated from another, update it
	if val, ok := resolveAnnotation(meta, ReplicateFromAnnotation); ok {
		log.Printf("%s %s is replicated from %s", r.Name, key, val)
		// update the dependencies of the source, even if it maybe does not exist yet
		if _, ok := r.targetsFrom[val]; !ok {
			r.targetsFrom[val] = make([]string, 0, 1)
		}
		r.targetsFrom[val] = append(r.targetsFrom[val], key)

		if sourceObject, exists, err := r.objectStore.GetByKey(val); err != nil {
			log.Printf("could not get %s %s: %s", r.Name, val, err)
			return
		// the source does not exist anymore/yet, clear the data of the target
		} else if !exists {
			log.Printf("source %s %s deleted: clearing target %s", r.Name, val, key)
			r.doClearObject(object)
		// update the target
		} else {
			r.replicateObject(object, sourceObject)
		}
	}
}

func (r *objectReplicator) replicateObject(object interface{}, sourceObject  interface{}) error {
	meta := r.getMeta(object)
	sourceMeta := r.getMeta(sourceObject)
	// make sure replication is allowed
	if ok, err := r.isReplicationAllowed(meta, sourceMeta); !ok {
		log.Printf("replication of %s %s/%s is cancelled: %s", r.Name, meta.Namespace, meta.Name, err)
		return err
	}
	// check if replication is needed
	if ok, _, err := r.needsDataUpdate(meta, sourceMeta); !ok {
		log.Printf("replication of %s %s/%s is skipped: %s", r.Name, meta.Namespace, meta.Name, err)
		return err
	}
	// replicate it
	return r.update(&r.replicatorProps, object, sourceObject)
}

func (r *objectReplicator) installObject(target string, targetObject interface{}, sourceObject interface{}) error {
	var targetMeta *metav1.ObjectMeta
	sourceMeta := r.getMeta(sourceObject)
	var targetSplit []string // similar to target, but splitted in 2
	// targetObject was not passed, check if it exists
	if targetObject == nil {
		targetSplit = strings.SplitN(target, "/", 2)
		// invalid target
		if len(targetSplit) != 2 {
			err := fmt.Errorf("illformed annotation %s in %s %s/%s: expected namespace/name, got %s",
				ReplicatedByAnnotation, r.Name, sourceMeta.Namespace, sourceMeta.Name, target)
			log.Printf("%s", err)
			return err
		}
		// error while getting the target
		if obj, exists, err := r.objectStore.GetByKey(target); err != nil {
			log.Printf("could not get %s %s: %s", r.Name, target, err)
			return err
		// the target exists already
		} else if exists {
			// update related objects
			targetObject = obj
			targetMeta = r.getMeta(targetObject)
			// check if target was created by replication from source
			if ok, err := r.isReplicatedBy(targetMeta, sourceMeta); !ok {
				log.Printf("replication of %s %s/%s is cancelled: %s",
					r.Name, sourceMeta.Namespace, sourceMeta.Name, err)
				return err
			}
		}
	// targetObject was passed already
	} else {
		targetMeta = r.getMeta(targetObject)
		targetSplit = []string{targetMeta.Namespace, targetMeta.Name}
	}
	// the data must come from another object
	if source, ok := resolveAnnotation(sourceMeta, ReplicateFromAnnotation); ok {
		if targetMeta != nil {
			// Check if needs an annotations update
			if ok, err := r.needsFromAnnotationsUpdate(targetMeta, sourceMeta); err != nil {
				log.Printf("replication of %s %s/%s is cancelled: %s",
					r.Name, sourceMeta.Namespace, sourceMeta.Name, err)
				return err

			} else if !ok {
				return nil
			}
		}
		// create a new meta with all the annotations
		copyMeta := metav1.ObjectMeta{
			Namespace:   targetSplit[0],
			Name:        targetSplit[1],
			Annotations: map[string]string{},
		}

		copyMeta.Annotations[ReplicatedByAnnotation] = fmt.Sprintf("%s/%s",
			sourceMeta.Namespace, sourceMeta.Name)
		copyMeta.Annotations[ReplicateFromAnnotation] = source
		if val, ok := sourceMeta.Annotations[ReplicateOnceAnnotation]; ok {
			copyMeta.Annotations[ReplicateOnceAnnotation] = val
		}
		// Needs ResourceVersion for update
		if targetMeta != nil {
			copyMeta.ResourceVersion = targetMeta.ResourceVersion
		}

		log.Printf("installing %s %s/%s: updating replicate-from annotations", r.Name, copyMeta.Namespace, copyMeta.Name)
		// install it, but keeps the original data
		return r.install(&r.replicatorProps, &copyMeta, sourceObject, targetObject)
	}
	// the data comes directly from the source
	if targetMeta != nil {
		// the target was previously replicated from another source
		// replication is required
		if _, ok := targetMeta.Annotations[ReplicateFromAnnotation]; ok {
		// checks that the target is up to date
		} else if ok, once, err := r.needsDataUpdate(targetMeta, sourceMeta); !ok {
			// check that the target needs replication-allowed annoations update
			if (!once) {
			} else if ok, err2 := r.needsAllowedAnnotationsUpdate(targetMeta, sourceMeta); err2 != nil {
				err = err2
			} else if ok {
				err = nil
			}
			if (err != nil) {
				log.Printf("replication of %s %s/%s is skipped: %s",
					r.Name, sourceMeta.Namespace, sourceMeta.Name, err)
				return err
			}
			// copy the target but update replication-allowed annoations
			copyMeta := targetMeta.DeepCopy()
			if val, ok := sourceMeta.Annotations[ReplicationAllowed]; ok {
				copyMeta.Annotations[ReplicationAllowed] = val
			} else {
				delete(copyMeta.Annotations, ReplicationAllowed)
			}
			if val, ok := sourceMeta.Annotations[ReplicationAllowedNamespaces]; ok {
				copyMeta.Annotations[ReplicationAllowedNamespaces] = val
			} else {
				delete(copyMeta.Annotations, ReplicationAllowedNamespaces)
			}

			log.Printf("installing %s %s/%s: updating replication-allowed annotations", r.Name, copyMeta.Namespace, copyMeta.Name)
			// install it with the original data
			return r.install(&r.replicatorProps, copyMeta, sourceObject, targetObject)
		}
	}
	// create a new meta with all the annotations
	copyMeta := metav1.ObjectMeta{
		Namespace:   targetSplit[0],
		Name:        targetSplit[1],
		Annotations: map[string]string{},
	}

	copyMeta.Annotations[ReplicatedAtAnnotation] = time.Now().Format(time.RFC3339)
	copyMeta.Annotations[ReplicatedByAnnotation] = fmt.Sprintf("%s/%s",
		sourceMeta.Namespace, sourceMeta.Name)
	copyMeta.Annotations[ReplicatedFromVersionAnnotation] = sourceMeta.ResourceVersion
	if val, ok := sourceMeta.Annotations[ReplicateOnceVersionAnnotation]; ok {
		copyMeta.Annotations[ReplicateOnceVersionAnnotation] = val
	}
	// replicate authorization annotations too
	if val, ok := sourceMeta.Annotations[ReplicationAllowed]; ok {
		copyMeta.Annotations[ReplicationAllowed] = val
	}
	if val, ok := sourceMeta.Annotations[ReplicationAllowedNamespaces]; ok {
		copyMeta.Annotations[ReplicationAllowedNamespaces] = val
	}
	// Needs ResourceVersion for update
	if targetMeta != nil {
		copyMeta.ResourceVersion = targetMeta.ResourceVersion
	}

	log.Printf("installing %s %s/%s: updating data", r.Name, copyMeta.Namespace, copyMeta.Name)
	// install it with the source data
	return r.install(&r.replicatorProps, &copyMeta, sourceObject, sourceObject)
}

func (r *objectReplicator) objectFromStore(key string) (interface{}, *metav1.ObjectMeta, error) {
	if object, exists, err := r.objectStore.GetByKey(key); err != nil {
		return nil, nil, fmt.Errorf("could not get %s %s: %s", r.Name, key, err)
	} else if !exists {
		return nil, nil, fmt.Errorf("could not get %s %s: does not exist", r.Name, key)
	} else {
		return object, r.getMeta(object), nil
	}
}

func (r *objectReplicator) updateDependents(object interface{}, replicas []string) error {
	meta := r.getMeta(object)
	key := fmt.Sprintf("%s/%s", meta.Namespace, meta.Name)

	sort.Strings(replicas)
	updatedReplicas := make([]string, 0, 0)
	var previous string

	for _, dependentKey := range replicas {
		// get rid of dupplicates in replicas
		if previous == dependentKey {
			continue
		}
		previous = dependentKey

		targetObject, targetMeta, err := r.objectFromStore(dependentKey)
		if err != nil {
			log.Printf("could not load dependent %s: %s", r.Name, err)
			continue
		}

		if val, ok := resolveAnnotation(targetMeta, ReplicateFromAnnotation); !ok || val != key {
			log.Printf("annotation of dependent %s %s changed", r.Name, dependentKey)
			continue
		}

		updatedReplicas = append(updatedReplicas, dependentKey)

		r.replicateObject(targetObject, object)
	}

	if len(updatedReplicas) > 0 {
		r.targetsFrom[key] = updatedReplicas
	} else {
		delete(r.targetsFrom, key)
	}

	return nil
}

func (r *objectReplicator) ObjectDeleted(object interface{}) {
	meta := r.getMeta(object)
	key := fmt.Sprintf("%s/%s", meta.Namespace, meta.Name)
	// delete targets of replicate-to annotations
	if targets, ok := r.targetsTo[key]; ok {
		for _, t := range targets {
			r.deleteObject(t, object)
		}
	}
	delete(r.targetsTo, key)
	delete(r.watchedTargets, key)
	delete(r.watchedPatterns, key)
	// clear targets of replicate-from annotations
	if replicas, ok := r.targetsFrom[key]; ok {
		sort.Strings(replicas)
		updatedReplicas := make([]string, 0, 0)
		var previous string

		for _, dependentKey := range replicas {
			// get rid of dupplicates in replicas
			if previous == dependentKey {
				continue
			}
			previous = dependentKey

			if ok, _ := r.clearObject(dependentKey, object); ok {
				updatedReplicas = append(updatedReplicas, dependentKey)
			}
		}

		if len(updatedReplicas) > 0 {
			r.targetsFrom[key] = updatedReplicas
		} else {
			delete(r.targetsFrom, key)
		}
	}
	// find which source want to replicate into this object, now that they can
	todo := map[string]bool{}

	for source, watched := range r.watchedTargets {
		for _, t := range watched {
			if key == t {
				todo[source] = true
				break
			}
		}
	}

	for source, patterns := range r.watchedPatterns {
		if todo[source] {
			continue
		}

		for _, p := range patterns {
			if p.Match(meta) {
				todo[source] = true
				break
			}
		}
	}
	// find the first source that still wants to replicate
	for source := range todo {
		if sourceObject, exists, err := r.objectStore.GetByKey(source); err != nil {
			log.Printf("could not get %s %s: %s", r.Name, source, err)
		// it should not happen, but maybe `ObjectDeleted` hasn't been called yet
		// just clean watched targets to avoid this to happen again
		} else if !exists {
			log.Printf("%s %s not found", r.Name, source)
			delete(r.watchedTargets, source)
			delete(r.watchedPatterns, source)

		} else if ok, err := r.isReplicatedTo(r.getMeta(sourceObject), meta); err != nil {
			log.Printf("could not parse %s %s: %s", r.Name, source, err)
		// the source sitll want to be replicated, so let's do it
		} else if ok {
			r.installObject(key, nil, sourceObject)
			break
		}
	}
}

func (r *objectReplicator) clearObject(key string, sourceObject interface{}) (bool, error) {
	sourceMeta := r.getMeta(sourceObject)

	targetObject, targetMeta, err := r.objectFromStore(key)
	if err != nil {
		log.Printf("could not load dependent %s: %s", r.Name, err)
		return false, err
	}

	if !annotationRefersTo(targetMeta, ReplicateFromAnnotation, sourceMeta) {
		log.Printf("annotation of dependent %s %s changed", r.Name, key)
		return false, nil
	}

	return true, r.doClearObject(targetObject)
}

func (r *objectReplicator) doClearObject(object interface{}) error {
	meta := r.getMeta(object)

	if _, ok := meta.Annotations[ReplicatedFromVersionAnnotation]; !ok {
		log.Printf("%s %s/%s is already up-to-date", r.Name, meta.Namespace, meta.Name)
		return nil
	}

	return r.clear(&r.replicatorProps, object)
}

func (r *objectReplicator) deleteObject(key string, sourceObject interface{}) (bool, error) {
	sourceMeta := r.getMeta(sourceObject)

	object, meta, err := r.objectFromStore(key)
	if err != nil {
		log.Printf("could not get %s %s: %s", r.Name, key, err)
		return false, err
	}

	// make sure replication is allowed
	if ok, err := r.isReplicatedBy(meta, sourceMeta); !ok {
		log.Printf("deletion of %s %s is cancelled: %s", r.Name, key, err)
		return false, err
	// delete the object
	} else {
		return true, r.doDeleteObject(object)
	}
}

func (r *objectReplicator) doDeleteObject(object interface{}) error {
	return r.delete(&r.replicatorProps, object)
}
