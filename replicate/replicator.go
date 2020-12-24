package replicate

import (
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
	"time"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

type replicatorActions interface {
	// Returns the ObjectMeta of the given object
	getMeta(object interface{}) *metav1.ObjectMeta
	// Updates object with the data from sourceObject and the given annotations
	update(r *replicatorProps, object interface{}, sourceObject interface{}, annotations map[string]string) (interface{}, error)
	// Clears all the data form object and apply the given annotations
	clear(r *replicatorProps, object interface{}, annotations map[string]string) (interface{}, error)
	// Creates or replaces an object, with the given meta, create by the given
	// sourceObject, and using the data of the given dataObject
	install(r *replicatorProps, meta *metav1.ObjectMeta, sourceObject interface{}, dataObject interface{}) (interface{}, error)
	// Deletes the given object
	delete(r *replicatorProps, object interface{}) error
}

type objectReplicator struct {
	replicatorProps
	replicatorActions
}

func (r *objectReplicator) Synced() bool {
	return r.namespaceController.HasSynced() && r.objectController.HasSynced()
}

func (r *objectReplicator) Start() func() {
	log.Printf("running %s object controller", r.Name)
	namespaceStop := make(chan struct{}, 1)
	objectStop := make(chan struct{}, 1)
	go r.namespaceController.Run(namespaceStop)
	go r.objectController.Run(objectStop)
	return func () {
		log.Printf("stopping %s object controller", r.Name)
		// stop := fmt.Errorf("stopping %s object controller", r.Name)
		namespaceStop <- struct{}{}
		objectStop <- struct{}{}
	}
}

// Inits the structure with empty maps
func (r *objectReplicator) InitStructure() {
	r.targetsFrom = make(map[string][]string)
	r.targetsTo = make(map[string][]string)

	r.watchedTargets = make(map[string][]string)
	r.watchedPatterns = make(map[string][]targetPattern)
}

// Inits the namespace store
func (r *objectReplicator) InitNamespaceStore(resyncPeriod time.Duration) {
	namespaceStore, namespaceController := cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(lo metav1.ListOptions) (runtime.Object, error) {
				list, err := r.client.CoreV1().Namespaces().List(lo)
				if err != nil {
					return list, err
				}
				// populate the store already, to avoid believing some items are deleted
				copy := make([]interface{}, len(list.Items))
				for index := range list.Items {
					copy[index] = &list.Items[index]
				}
				r.namespaceStore.Replace(copy, "init")
				return list, err
			},
			WatchFunc: func(lo metav1.ListOptions) (watch.Interface, error) {
				return r.client.CoreV1().Namespaces().Watch(lo)
			},
		},
		&v1.Namespace{},
		resyncPeriod,
		cache.ResourceEventHandlerFuncs {
			AddFunc:    r.NamespaceAdded,
			UpdateFunc: func(old interface{}, new interface{}) {},
			DeleteFunc: func(obj interface{}) {},
		},
	)

	r.namespaceStore = namespaceStore
	r.namespaceController = namespaceController
}

// Inites the object store
// This is the only part using reflect's dark magic to simplify the process
//
// The usual call is:
// 		r.InitObjectStore(resyncPeriod, client.CoreV1().MyType(""), &v1.MyType{})
func (r *objectReplicator) InitObjectStore(resyncPeriod time.Duration, objectsInterface interface{}, objectType runtime.Object) {
	objects := reflect.ValueOf(objectsInterface)
	listFunc := objects.MethodByName("List")
	watchFunc := objects.MethodByName("Watch")

	objectStore, objectController := cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(lo metav1.ListOptions) (runtime.Object, error) {
				// list, err := objectsInterface.List(lo)
				resp := listFunc.Call([]reflect.Value{reflect.ValueOf(lo)})
				if !resp[1].IsNil() {
					return nil, resp[1].Interface().(error)
				}
				// populate the store already, to avoid believing some items are deleted
				// for index := range list.Items {
				// 	copy[index] = &list.Items[index]
				// }
				items := resp[0].Elem().FieldByName("Items")
				length := items.Len()
				copy := make([]interface{}, length)
				for index := 0; index < length; index ++ {
					copy[index] = items.Index(index).Addr().Interface()
				}
				r.objectStore.Replace(copy, "init")
				// return list, err
				return resp[0].Interface().(runtime.Object), nil
			},
			WatchFunc: func(lo metav1.ListOptions) (watch.Interface, error) {
				// watch, err := objectsInterface.Watch(lo)
				resp := watchFunc.Call([]reflect.Value{reflect.ValueOf(lo)})
				if !resp[1].IsNil() {
					return nil, resp[1].Interface().(error)
				}
				// return watch, err
				return resp[0].Interface().(watch.Interface), nil
			},
		},
		objectType,
		resyncPeriod,
		cache.ResourceEventHandlerFuncs {
			AddFunc:    r.ObjectAdded,
			UpdateFunc: func(old interface{}, new interface{}) { r.ObjectAdded(new) },
			DeleteFunc: r.ObjectDeleted,
		},
	)

	r.objectStore = objectStore
	r.objectController = objectController
}

// Inits the the replicator
//
// The usual call is:
// 		r.Init(resyncPeriod, client.CoreV1().MyType(""), &v1.MyType{})
func (r *objectReplicator) Init(resyncPeriod time.Duration, objectsInterface interface{}, objectType runtime.Object) {
	r.InitStructure()
	r.InitNamespaceStore(resyncPeriod)
	if objectsInterface != nil {
		r.InitObjectStore(resyncPeriod, objectsInterface, objectType)
	}
}

// Called when a new namespace was created
// Checks if any object wants to replicte itself into it
func (r *objectReplicator) NamespaceAdded(object interface{}) {
	namespace := object.(*v1.Namespace)
	log.Printf("namespace added %s", namespace.Name)
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
		if sourceObject, _, err := r.objectFromStore(source, true); err != nil {
			log.Printf("could not get source %s %s: %s", r.Name, source, err)
		// let the source replicate
		} else {
			log.Printf("%s %s is watching namespace %s", r.Name, source, namespace.Name)
			r.replicateToNamespace(sourceObject, namespace.Name)
		}
	}
}

// Replicates an object to a new namespace
func (r *objectReplicator) replicateToNamespace(object interface{}, namespace string) {
	meta := r.getMeta(object)
	key := fmt.Sprintf("%s/%s", meta.Namespace, meta.Name)
	// those annotations have priority
	// forget about the "replicate-to" annotations
	if _, ok := meta.Annotations[CreatedByAnnotation]; ok {
		log.Printf("source %s %s is already created by another %s", r.Name, key, r.Name)
		delete(r.watchedTargets, key)
		delete(r.watchedPatterns, key)
		return
	}
	// get all targets
	targets, targetPatterns, err := r.getReplicationTargets(meta)
	// annotations got invalid, clear these fields
	if err != nil {
		log.Printf("could not parse %s %s: %s", r.Name, key, err)
		delete(r.watchedTargets, key)
		delete(r.watchedPatterns, key)
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

// Called when an object was created or updated
// Has to check for many cases:
//	- Maybe it wants to start replicating
//	- Maybe it wants to stop replicating
//	- Maybe it wants a copy of another objet
//	- Maybe it is copied by another object
// This function can be called any number of time, in any order, and must still
// produce the same effect. In patrticular, any object triggered by the
// replicator will trigger another call to this function.
func (r *objectReplicator) ObjectAdded(object interface{}) {
	meta := r.getMeta(object)
	key := fmt.Sprintf("%s/%s", meta.Namespace, meta.Name)
	log.Printf("%s added %s", r.Name, key)
	// clean all those fields, they will be refilled further anyway
	delete(r.watchedTargets, key)
	delete(r.watchedPatterns, key)
	// annotations are invalid, ignore this object
	if update, err := updateDeprecatedAnnotations(meta); err != nil {
		log.Printf("could not parse %s %s: %s", r.Name, key, err)
		return
	// annotations are deprecated and should be updated
	} else if update {
		log.Printf("updating %s %s: updating deprecated annotations", r.Name, key)
		// copy the annotation without CheckedAnnotation
		annotations := map[string]string{}
		for k, v := range meta.Annotations {
			annotations[k] = v
		}
		delete(annotations, CheckedAnnotation)
		// update it
		object, err = r.update(&r.replicatorProps, object, object, annotations)
		if err != nil {
			log.Printf("error while updating %s %s: %s", r.Name, key, err)
		// update is successful, save the vew object
		} else {
			r.objectStore.Update(object)
			meta = r.getMeta(object)
			// that should never happen as kubernetes should send back
			// the annotations as they were received
			if _, err := updateDeprecatedAnnotations(meta); err != nil {
				log.Printf("could not parse %s %s: %s", r.Name, key, err)
				return
			}
		}
	}
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
		Targets: for _, target := range oldTargets {
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
			r.deleteObject(target, object,
				"source has updated \"replicate-to\" annotations")
		}
		// will be refilled further
		delete(r.targetsTo, key)
	}
	// check for object having dependencies, and update them
	if replicas, ok := r.targetsFrom[key]; ok && len(replicas) > 0 {
		log.Printf("%s %s has %d dependents", r.Name, key, len(replicas))
		// sort the replicas to get rid of duplicates
		sort.Strings(replicas)
		updatedReplicas := []string{}
		var previous string

		for _, dependent := range replicas {
			// get rid of dupplicates in replicas
			if previous == dependent {
				continue
			}
			previous = dependent
			// get the target
			targetObject, targetMeta, err := r.objectFromStore(dependent, true)
			if err != nil {
				log.Printf("could not get dependent %s %s: %s", r.Name, dependent, err)
				continue
			}
			// check that the target still wants to replicate the object
			if val, ok := resolveAnnotation(targetMeta, ReplicationSourceAnnotation); !ok || val != key {
				log.Printf("annotation of dependent %s %s changed", r.Name, dependent)
				continue
			}
			// this target still wants to replicate the object
			updatedReplicas = append(updatedReplicas, dependent)
			// replicate the object again
			r.replicateObject(targetObject, object)
		}
		// save the new replicas list
		if len(updatedReplicas) > 0 {
			r.targetsFrom[key] = updatedReplicas
		} else {
			delete(r.targetsFrom, key)
		}
	}
	// this object was replicated by another, update it
	if val, ok := meta.Annotations[CreatedByAnnotation]; ok {
		log.Printf("%s %s is replicated by %s", r.Name, key, val)
		sourceObject, sourceMeta, err := r.objectFromStore(val, false)
		reason := ""

		if err != nil {
			log.Printf("could not get source %s %s: %s", r.Name, val, err)
			return
		// the source has been deleted, so should this object be
		} else if sourceObject == nil {
			log.Printf("source %s %s deleted: deleting target %s", r.Name, val, key)
			reason = "source does not exist"

		} else if ok, err := r.isReplicatedTo(sourceMeta, meta); err != nil {
			log.Printf("could not parse %s %s: %s", r.Name, val, err)
			return
		// the source annotations have changed, this replication is deleted
		} else if !ok {
			log.Printf("source %s %s is not replicated to %s: deleting target", r.Name, val, key)
			sourceObject = nil
			reason = "source has changed \"replicate-to\" annotations"
		}
		// no source, delete it
		if sourceObject == nil {
			r.doDeleteObject(meta, object, reason)
			return
		// source is here, install it
		} else if err := r.installObject("", object, sourceObject); err != nil {
			return
		// get it back after edit
		} else if obj, m, err := r.objectFromStore(key, true); err != nil {
			log.Printf("could not get %s %s: %s", r.Name, key, err)
			return
		// continue
		} else {
			object = obj
			meta = m
			targets = nil
			targetPatterns = nil
		}
	// this object is replicated to other locations
	// it cannot be both replicated by an object and replicated to another
	} else if targets != nil || targetPatterns != nil {
		// all the namespaces
		namespaces := r.namespaceStore.ListKeys()
		existsNamespaces := map[string]bool{}
		for _, ns := range namespaces {
			existsNamespaces[ns] = true
		}
		// the slice of all the target this object should replicate to
		existingTargets := []string{}

		for _, t := range(targets) {
			ns := strings.SplitN(t, "/", 2)[0]
			if existsNamespaces[ns] {
				existingTargets = append(existingTargets, t)
			} else {
				log.Printf("replication of %s %s to %s cancelled: no namespace %s",
					r.Name, key, t, ns)
			}
		}

		if len(targetPatterns) > 0 {
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
	if val, ok := resolveAnnotation(meta, ReplicationSourceAnnotation); ok {
		log.Printf("%s %s is replicated from %s", r.Name, key, val)
		// update the dependencies of the source, even if it maybe does not exist yet
		if _, ok := r.targetsFrom[val]; !ok {
			r.targetsFrom[val] = make([]string, 0, 1)
		}
		r.targetsFrom[val] = append(r.targetsFrom[val], key)

		if sourceObject, _, err := r.objectFromStore(val, false); err != nil {
			log.Printf("could not get source %s %s: %s", r.Name, val, err)
			return
		// the source does not exist anymore/yet, clear the data of the target
		} else if sourceObject == nil {
			log.Printf("source %s %s deleted: clearing target %s", r.Name, val, key)
			r.doClearObject(object, "source does not exist")
		// update the target
		} else {
			r.replicateObject(object, sourceObject)
		}
	}
}

// An object requests to have a copy of another usinf the "replicate-from" annotation
// Updates the data of an object with the data from the source
func (r *objectReplicator) replicateObject(object interface{}, sourceObject interface{}) error {
	meta := r.getMeta(object)
	sourceMeta := r.getMeta(sourceObject)
	_, replicated := meta.Annotations[ReplicatedVersionAnnotation]
	// make sure replication is allowed
	if ok, disallowed, err := r.isReplicationAllowedAnnotation(meta, sourceMeta); !ok {
		log.Printf("replication of %s %s/%s is cancelled: %s", r.Name, meta.Namespace, meta.Name, err)
		if disallowed && replicated {
			return r.doClearObject(object, "source disallowed")
		} else {
			return err
		}
	}
	// check if replication is needed
	if ok, _, err := r.needsDataUpdate(meta, sourceMeta); !ok {
		log.Printf("replication of %s %s/%s is skipped: %s", r.Name, meta.Namespace, meta.Name, err)
		return err
	}
	// Copy and update the annotations
	annotations := map[string]string{}
	for k, v := range meta.Annotations {
		annotations[k] = v
	}
	delete(annotations, CheckedAnnotation)
	annotations[ReplicationTimeAnnotation] = time.Now().Format(time.RFC3339)
	annotations[ReplicatedVersionAnnotation] = sourceMeta.ResourceVersion
	if val, ok := sourceMeta.Annotations[ReplicateOnceVersionAnnotation]; ok {
		annotations[ReplicateOnceVersionAnnotation] = val
	} else {
		delete(annotations, ReplicateOnceVersionAnnotation)
	}
	// replicate it
	log.Printf("updating %s %s/%s: updating data", r.Name, meta.Namespace, meta.Name)
	object, err := r.update(&r.replicatorProps, object, sourceObject, annotations)
	if err != nil {
		log.Printf("error while updating %s %s/%s: %s", r.Name, meta.Namespace, meta.Name, err)
	} else {
		// update the object in the store as soon as possible
		r.objectStore.Update(object)
	}
	return err
}

// Replicates a source object to a target location
// Can pass either `target` as the address of  the new object,
// either `targetObject` as the current state of the target.
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
				CreatedByAnnotation, r.Name, sourceMeta.Namespace, sourceMeta.Name, target)
			log.Printf("%s", err)
			return err
		}
		// error while getting the target
		if obj, meta, err := r.objectFromStore(target, false); err != nil {
			log.Printf("could not get target %s %s: %s", r.Name, target, err)
			return err
		// the target exists already
		} else if obj != nil {
			// update related objects
			targetObject = obj
			targetMeta = meta
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

	reason := ""
	// the data must come from another object
	if source, ok := resolveAnnotation(sourceMeta, ReplicationSourceAnnotation); ok {
		if targetMeta != nil {
			// Check if needs an annotations update
			if ok, err := r.needsFromAnnotationsUpdate(targetMeta, sourceMeta); err != nil {
				log.Printf("replication of %s %s/%s is cancelled: %s",
					r.Name, sourceMeta.Namespace, sourceMeta.Name, err)
				return err

			} else if !ok {
				return nil
			}
			reason = "creating with \"replicate-from\" annotations"
		} else {
			reason = "updating \"replicate-from\" annotations"
		}
		// create a new meta with all the annotations
		copyMeta := metav1.ObjectMeta{
			Namespace:   targetSplit[0],
			Name:        targetSplit[1],
			Labels:      getCopyLabels(),
			Annotations: map[string]string{},
		}

		copyMeta.Annotations[CreatedByAnnotation] = fmt.Sprintf("%s/%s",
			sourceMeta.Namespace, sourceMeta.Name)
		copyMeta.Annotations[ReplicationSourceAnnotation] = source
		if val, ok := sourceMeta.Annotations[ReplicateOnceAnnotation]; ok {
			copyMeta.Annotations[ReplicateOnceAnnotation] = val
		}
		// Needs ResourceVersion for update
		if targetMeta != nil {
			copyMeta.ResourceVersion = targetMeta.ResourceVersion
		}
		// install it, but keeps the original data
		return r.doInstallObject(&copyMeta, sourceObject, targetObject, reason)
	}
	// the data comes directly from the source
	if targetMeta != nil {
		// the target was previously replicated from another source
		// replication is required
		if _, ok := targetMeta.Annotations[ReplicationSourceAnnotation]; ok {
		// checks that the target is up to date
		} else if ok, once, err := r.needsDataUpdate(targetMeta, sourceMeta); !ok {
			// check that the target needs replication-allowed annotations update
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
			// copy the target but update replication-allowed annotations
			copyMeta := targetMeta.DeepCopy()
			copyMeta.Labels = getCopyLabels()
			if val, ok := sourceMeta.Annotations[ReplicationAllowedAnnotation]; ok {
				copyMeta.Annotations[ReplicationAllowedAnnotation] = val
			} else {
				delete(copyMeta.Annotations, ReplicationAllowedAnnotation)
			}
			if val, ok := sourceMeta.Annotations[AllowedNamespacesAnnotation]; ok {
				copyMeta.Annotations[AllowedNamespacesAnnotation] = val
			} else {
				delete(copyMeta.Annotations, AllowedNamespacesAnnotation)
			}
			// install it with the original data
			reason = "updating \"replication-allowed\" annotations"
			return r.doInstallObject(copyMeta, sourceObject, targetObject, reason)
		} else {
			reason = "updating data"
		}
	} else {
		reason = "creating with data"
	}
	// create a new meta with all the annotations
	copyMeta := metav1.ObjectMeta{
		Namespace:   targetSplit[0],
		Name:        targetSplit[1],
		Labels:      getCopyLabels(),
		Annotations: map[string]string{},
	}

	copyMeta.Annotations[ReplicationTimeAnnotation] = time.Now().Format(time.RFC3339)
	copyMeta.Annotations[CreatedByAnnotation] = fmt.Sprintf("%s/%s",
		sourceMeta.Namespace, sourceMeta.Name)
	copyMeta.Annotations[ReplicatedVersionAnnotation] = sourceMeta.ResourceVersion
	if val, ok := sourceMeta.Annotations[ReplicateOnceVersionAnnotation]; ok {
		copyMeta.Annotations[ReplicateOnceVersionAnnotation] = val
	}
	// replicate authorization annotations too
	if val, ok := sourceMeta.Annotations[ReplicationAllowedAnnotation]; ok {
		copyMeta.Annotations[ReplicationAllowedAnnotation] = val
	}
	if val, ok := sourceMeta.Annotations[AllowedNamespacesAnnotation]; ok {
		copyMeta.Annotations[AllowedNamespacesAnnotation] = val
	}
	// Needs ResourceVersion for update
	if targetMeta != nil {
		copyMeta.ResourceVersion = targetMeta.ResourceVersion
	}
	// install it with the source data
	return r.doInstallObject(&copyMeta, sourceObject, sourceObject, reason)
}

// A wrapper around replicatorActions.install
func (r *objectReplicator) doInstallObject(meta *metav1.ObjectMeta, sourceObject interface{}, dataObject interface{}, reason string) error {
	log.Printf("installing %s %s/%s: %s", r.Name, meta.Namespace, meta.Name, reason)
	// clear the checked annotation
	delete(meta.Annotations, CheckedAnnotation)
	object, err := r.install(&r.replicatorProps, meta, sourceObject, dataObject)
	if err != nil {
		log.Printf("error while installing %s %s/%s: %s", r.Name, meta.Namespace, meta.Name, err)
	} else {
		// update the object in the store as soon as possible
		r.objectStore.Update(object)
	}
	return err
}

// Returns an object from the store with its meta object
// If it doesn't exist, returns an error if mustExist, else return nil
func (r *objectReplicator) objectFromStore(key string, mustExist bool) (interface{}, *metav1.ObjectMeta, error) {
	object, exists, err := r.objectStore.GetByKey(key)
	if err != nil {
		return nil, nil, err
	// the object does not exists, these field should neither
	} else if !exists {
		delete(r.watchedTargets, key)
		delete(r.watchedPatterns, key)
		// reply with an error if mustExist
		if mustExist {
			err = fmt.Errorf("does not exist")
		}
		return nil, nil, err
	}
	meta := r.getMeta(object)
	// annotations are wrong, clear those fields
	if _, err := updateDeprecatedAnnotations(meta); err != nil {
		delete(r.watchedTargets, key)
		delete(r.watchedPatterns, key)
		return nil, nil, err
	}
	return object, meta, nil
}

// Called when an object is deleted
// Has to check for several cases:
//	- The object was replicated by other objects
//	- The object was replicating itself at other locations
//  - Another object wants to replicate at this location
func (r *objectReplicator) ObjectDeleted(object interface{}) {
	meta := r.getMeta(object)
	key := fmt.Sprintf("%s/%s", meta.Namespace, meta.Name)
	// log.Printf("%s deleted %s", r.Name, key)
	// delete targets of replicate-to annotations
	if targets, ok := r.targetsTo[key]; ok {
		for _, t := range targets {
			r.deleteObject(t, object, "source deleted")
		}
	}
	delete(r.targetsTo, key)
	delete(r.watchedTargets, key)
	delete(r.watchedPatterns, key)
	// clear targets of replicate-from annotations
	if replicas, ok := r.targetsFrom[key]; ok {
		// sort the replicas to get rid of the duplicates
		sort.Strings(replicas)
		updatedReplicas := make([]string, 0, 0)
		var previous string

		for _, dependentKey := range replicas {
			// get rid of duplicates in replicas
			if previous == dependentKey {
				continue
			}
			previous = dependentKey
			// the target is cleared, but we still want to replicate
			// when the source is created again
			if ok, _ := r.clearObject(dependentKey, object); ok {
				updatedReplicas = append(updatedReplicas, dependentKey)
			}
		}
		// save the new replicas list
		if len(updatedReplicas) > 0 {
			r.targetsFrom[key] = updatedReplicas
		} else {
			delete(r.targetsFrom, key)
		}
	}
	// If the namespace of the object does not exist anymore, cannot be replaced
	if _, exists, err := r.namespaceStore.GetByKey(meta.Namespace); err != nil {
		log.Printf("could not get namespace %s: %s", meta.Namespace, err)
		return
	} else if !exists {
		return
	}
	// find which source want to replicate at this location, now that it's free
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
		if sourceObject, sourceMeta, err := r.objectFromStore(source, true); err != nil {
			log.Printf("could not get source %s %s: %s", r.Name, source, err)
		// annotations of the source got wrong, clear the fields
		} else if ok, err := r.isReplicatedTo(sourceMeta, meta); err != nil {
			delete(r.watchedTargets, source)
			delete(r.watchedPatterns, source)
			log.Printf("could not parse %s %s: %s", r.Name, source, err)
		// the source sitll want to be replicated, so let's do it
		} else if ok {
			r.installObject(key, nil, sourceObject)
			break
		}
	}
}

// Clears all the data from an object
// But first check if the target is still replicating the source
func (r *objectReplicator) clearObject(key string, sourceObject interface{}) (bool, error) {
	sourceMeta := r.getMeta(sourceObject)

	targetObject, targetMeta, err := r.objectFromStore(key, true)
	if err != nil {
		log.Printf("could not load dependent %s: %s", r.Name, err)
		return false, err
	}

	if !annotationRefersTo(targetMeta, ReplicationSourceAnnotation, sourceMeta) {
		log.Printf("annotation of dependent %s %s changed", r.Name, key)
		return false, nil
	}

	return true, r.doClearObject(targetObject, "source deleted")
}

// Clears all the data from an object
func (r *objectReplicator) doClearObject(object interface{}, reason string) error {
	meta := r.getMeta(object)

	if _, ok := meta.Annotations[ReplicatedVersionAnnotation]; !ok {
		log.Printf("%s %s/%s is already up-to-date", r.Name, meta.Namespace, meta.Name)
		return nil
	}
	// Copy and clean the annotations
	annotations := map[string]string{}
	for k, v := range meta.Annotations {
		annotations[k] = v
	}
	delete(annotations, CheckedAnnotation)
	annotations[ReplicationTimeAnnotation] = time.Now().Format(time.RFC3339)
	delete(annotations, ReplicatedVersionAnnotation)
	delete(annotations, ReplicateOnceVersionAnnotation)

	log.Printf("clearing %s %s/%s: %s", r.Name, meta.Namespace, meta.Name, reason)
	object, err := r.clear(&r.replicatorProps, object, annotations)
	if err != nil {
		log.Printf("error while clearing %s %s/%s: %s", r.Name, meta.Namespace, meta.Name, err)
	} else {
		// update the object in the store as soon as possible
		r.objectStore.Update(object)
	}
	return err
}

// Deletes an object that was creted by replication
// But first check that it is still the case
func (r *objectReplicator) deleteObject(key string, sourceObject interface{}, reason string) (bool, error) {
	sourceMeta := r.getMeta(sourceObject)

	object, meta, err := r.objectFromStore(key, true)
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
		return true, r.doDeleteObject(meta, object, reason)
	}
}

// A wrapper around replicatorActions.delete
func (r *objectReplicator) doDeleteObject(meta *metav1.ObjectMeta, object interface{}, reason string) error {
	log.Printf("deleting %s %s/%s: %s", r.Name, meta.Namespace, meta.Name, reason)
	err := r.delete(&r.replicatorProps, object)
	if err != nil {
		log.Printf("error while deleting %s %s/%s: %s", r.Name, meta.Namespace, meta.Name, err)
	} else {
		// update the object in the store as soon as possible
		r.objectStore.Delete(object)
	}
	return err
}
