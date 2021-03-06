package replicate

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	semver "github.com/Masterminds/semver/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// pattern of a valid kubernetes name
var validName = regexp.MustCompile(`^[0-9a-z.-]+$`)
var validPath = regexp.MustCompile(`^[0-9a-z.-]+/[0-9a-z.-]+$`)

// a struct representing a pattern to match namespaces and generating targets
type targetPattern struct {
	namespace *regexp.Regexp
	name      string
}
// if the pattern matches the given target object
func (pattern targetPattern) Match(object *metav1.ObjectMeta) bool {
	return object.Name == pattern.name && pattern.namespace.MatchString(object.Namespace)
}
// if the pattern matches the given target path
func (pattern targetPattern) MatchString(target string) bool {
	parts := strings.SplitN(target, "/", 2)
	return len(parts) == 2 && parts[1] == pattern.name && pattern.namespace.MatchString(parts[0])
}
// if the pattern matches the given namespace, returns a target path in this namespace
func (pattern targetPattern) MatchNamespace(namespace string) string {
	if pattern.namespace.MatchString(namespace) {
		return fmt.Sprintf("%s/%s", namespace, pattern.name)
	} else {
		return ""
	}
}
// returns a slice of targets paths in the given namespaces when matching
func (pattern targetPattern) Targets(namespaces []string) []string {
	suffix := "/" + pattern.name
	targets := []string{}
	for _, ns := range namespaces {
		if pattern.namespace.MatchString(ns) {
			targets = append(targets, ns+suffix)
		}
	}
	return targets
}

type replicatorProps struct {
	// displayed name for the resources
	Name                string
	// when true, "allowed" annotations are ignored
	allowAll            bool
	// the kubernetes client to use
	client              kubernetes.Interface

	// the store and controller for all the objects to watch replicate
	objectStore         cache.Store
	objectController    cache.Controller

	// the store and controller for the namespaces
	namespaceStore      cache.Store
	namespaceController cache.Controller

	// a {source => targets} map for the "replicate-from" annotation
	targetsFrom         map[string][]string
	// a {source => targets} map for the "replicate-to" annotation
	targetsTo           map[string][]string

	// a {source => targets} map for all the targeted objects
	watchedTargets   map[string][]string
	// a {source => targetPatterns} for all the targeted objects
	watchedPatterns   map[string][]targetPattern
}

// Replicator describes the common interface that the secret and configmap
// replicators should adhere to
type Replicator interface {
	Start()
	Synced() bool
}

// Checks if replication is allowed in annotations of the source object
// It means that replication-allowes and replications-allowed-namespaces are correct
// Returns true if replication is allowed.
// If replication is not allowed returns false with error message
func (r *replicatorProps) isReplicationAllowed(object *metav1.ObjectMeta, sourceObject *metav1.ObjectMeta) (bool, error) {
	annotationAllowed, ok := sourceObject.Annotations[ReplicationAllowed]
	annotationAllowedNs, okNs := sourceObject.Annotations[ReplicationAllowedNamespaces]
	// unless allowAll, explicit permission is required
	if !r.allowAll && !ok && !okNs {
		return false, fmt.Errorf("source %s/%s does not explicitely allow replication",
			sourceObject.Namespace, sourceObject.Name)
	}
	// check allow annotation
	if ok {
		if val, err := strconv.ParseBool(annotationAllowed); err != nil {
			return false, fmt.Errorf("source %s/%s has illformed annotation %s (%s): %s",
				sourceObject.Namespace, sourceObject.Name, ReplicationAllowed, annotationAllowed, err)
		} else if !val {
			return false, fmt.Errorf("source %s/%s explicitely disallow replication",
				sourceObject.Namespace, sourceObject.Name)
		}
	}
	// check allow-namespaces annotation
	if okNs {
		allowed := false
		for _, ns := range strings.Split(annotationAllowedNs, ",") {
			if ns == "" {
			} else if validName.MatchString(ns) {
				if ns == object.Namespace {
					allowed = true
				}
			} else if ok, err := regexp.MatchString(`^(?:`+ns+`)$`, object.Namespace); ok {
				allowed = true
			} else if err != nil {
				return false, fmt.Errorf("source %s/%s has compilation error on annotation %s (%s): %s",
					sourceObject.Namespace, sourceObject.Name, ReplicationAllowedNamespaces, ns, err)
			}
		}
		if !allowed {
			return false, fmt.Errorf("source %s/%s does not allow replication to namespace %s",
				sourceObject.Namespace, sourceObject.Name, object.Namespace)
		}
	}
	// source cannot have "replicate-from" annotation
	if val, ok := resolveAnnotation(sourceObject, ReplicateFromAnnotation); ok {
		return false, fmt.Errorf("source %s/%s is already replicated from %s",
			sourceObject.Namespace, sourceObject.Name, val)
	}

	return true, nil
}

// Checks that data update is needed
// Returns true if update is needed
// If update is not needed returns false with error message
func (r *replicatorProps) needsDataUpdate(object *metav1.ObjectMeta, sourceObject *metav1.ObjectMeta) (bool, bool, error) {
	// target was "replicated" from a delete source, or never replicated
	if targetVersion, ok := object.Annotations[ReplicatedFromVersionAnnotation]; !ok {
		return true, false, nil
	// target and source share the same version
	} else if ok && targetVersion == sourceObject.ResourceVersion {
		return false, false, fmt.Errorf("target %s/%s is already up-to-date", object.Namespace, object.Name)
	}

	hasOnce := false
	// no once annotation, nothing to check
	if annotationOnce, ok := sourceObject.Annotations[ReplicateOnceAnnotation]; !ok {
	// once annotation is not a boolean
	} else if once, err := strconv.ParseBool(annotationOnce); err != nil {
		return false, false, fmt.Errorf("source %s/%s has illformed annotation %s: %s",
			sourceObject.Namespace, sourceObject.Name, ReplicateOnceAnnotation, err)
	// once annotation is present
	} else if once {
		hasOnce = true
	}
	// no once annotation, nothing to check
	if annotationOnce, ok := object.Annotations[ReplicateOnceAnnotation]; !ok {
	// once annotation is not a boolean
	} else if once, err := strconv.ParseBool(annotationOnce); err != nil {
		return false, false, fmt.Errorf("target %s/%s has illformed annotation %s: %s",
			object.Namespace, object.Name, ReplicateOnceAnnotation, err)
	// once annotation is present
	} else if once {
		hasOnce = true
	}

	if !hasOnce {
	// no once version annotation in the source, only replicate once
	} else if annotationVersion, ok := sourceObject.Annotations[ReplicateOnceVersionAnnotation]; !ok {
	// once version annotation is not a valid version
	} else if sourceVersion, err := semver.NewVersion(annotationVersion); err != nil {
		return false, false, fmt.Errorf("source %s/%s has illformed annotation %s: %s",
			sourceObject.Namespace, sourceObject.Name, ReplicateOnceVersionAnnotation, err)
	// the source has a once version annotation but it is "0.0.0" anyway
	} else if version0, _ := semver.NewVersion("0"); sourceVersion.Equal(version0) {
	// no once version annotation in the target, should update
	} else if annotationVersion, ok := object.Annotations[ReplicateOnceVersionAnnotation]; !ok {
		hasOnce = false
	// once version annotation is not a valid version
	} else if targetVersion, err := semver.NewVersion(annotationVersion); err != nil {
		return false, false, fmt.Errorf("target %s/%s has illformed annotation %s: %s",
			object.Namespace, object.Name, ReplicateOnceVersionAnnotation, err)
	// source version is greatwe than source version, should update
	} else if sourceVersion.GreaterThan(targetVersion) {
		hasOnce = false
	// source version is not greater than target version
	} else {
		return false, true, fmt.Errorf("target %s/%s is already replicated once at version %s",
			object.Namespace, object.Name, sourceVersion)
	}

	if hasOnce {
		return false, true, fmt.Errorf("target %s/%s is already replicated once",
			object.Namespace, object.Name)
	}

	return true, false, nil
}

// Checks that data annotation is needed
// Returns true if update is needed
// Return an error only if a source annotation is illformed
func (r *replicatorProps) needsFromAnnotationsUpdate(object *metav1.ObjectMeta, sourceObject *metav1.ObjectMeta) (bool, error) {
	update := false
	// check "from" annotation of the source
	if source, sOk := resolveAnnotation(sourceObject, ReplicateFromAnnotation); !sOk {
		return false, fmt.Errorf("source %s/%s misses annotation %s",
			sourceObject.Namespace, sourceObject.Name, ReplicateFromAnnotation)

	} else if !validPath.MatchString(source) ||
			source == fmt.Sprintf("%s/%s", sourceObject.Namespace, sourceObject.Name) {
		return false, fmt.Errorf("source %s/%s has invalid annotation %s (%s)",
			sourceObject.Namespace, sourceObject.Name, ReplicateFromAnnotation, source)

	// check that target has the same annotation
	} else if val, ok := object.Annotations[ReplicateFromAnnotation]; !ok || val != source {
		update = true
	}

	source, sOk := sourceObject.Annotations[ReplicateOnceAnnotation]
	// check "once" annotation of the source
	if sOk {
		if _, err := strconv.ParseBool(source); err != nil {
			return false, fmt.Errorf("source %s/%s has illformed annotation %s: %s",
				sourceObject.Namespace, sourceObject.Name, ReplicateOnceAnnotation, err)
		}
	}
	// check that target has the same annotation
	if val, ok := object.Annotations[ReplicateOnceAnnotation]; sOk != ok || ok && val != source {
		update = true
	}

	return update, nil
}

func (r *replicatorProps) needsAllowedAnnotationsUpdate(object *metav1.ObjectMeta, sourceObject *metav1.ObjectMeta) (bool, error) {
	update := false

	allowed, okA := sourceObject.Annotations[ReplicationAllowed]
	if val, ok := object.Annotations[ReplicationAllowed]; ok != okA || ok && val != allowed {
		update = true
	}

	allowedNs, okNs := sourceObject.Annotations[ReplicationAllowedNamespaces]
	if val, ok := object.Annotations[ReplicationAllowedNamespaces]; ok != okNs || ok && val != allowedNs {
		update = true
	}

	if !update {
		return false, nil
	}

	// check allow annotation
	if okA {
		if _, err := strconv.ParseBool(allowed); err != nil {
			return false, fmt.Errorf("source %s/%s has illformed annotation %s (%s): %s",
				sourceObject.Namespace, sourceObject.Name, ReplicationAllowed, allowed, err)
		}
	}
	// check allow-namespaces annotation
	if okNs {
		for _, ns := range strings.Split(allowedNs, ",") {
			if ns == "" || validName.MatchString(ns) {
			} else if _, err := regexp.Compile(`^(?:`+ns+`)$`); err != nil {
				return false, fmt.Errorf("source %s/%s has compilation error on annotation %s (%s): %s",
					sourceObject.Namespace, sourceObject.Name, ReplicationAllowedNamespaces, ns, err)
			}
		}
	}

	return true, nil
}

// Checks that replication from the source object to the target objects is allowed
// It means that the target object was created using replication of the same source
// Returns true if replication is allowed
// If replication is not allowed returns false with error message
func (r *replicatorProps) isReplicatedBy(object *metav1.ObjectMeta, sourceObject *metav1.ObjectMeta) (bool, error) {
	// make sure that the target object was created from the source
	if annotationFrom, ok := object.Annotations[ReplicatedByAnnotation]; !ok {
		return false, fmt.Errorf("target %s/%s was not replicated",
			object.Namespace, object.Name)

	} else if annotationFrom != fmt.Sprintf("%s/%s", sourceObject.Namespace, sourceObject.Name) {
		return false, fmt.Errorf("target %s/%s was not replicated from %s/%s",
			object.Namespace, object.Name, sourceObject.Namespace, sourceObject.Name)
	}

	return true, nil
}


// Checks if the object is replicated to the target
// Returns an error only if the annotations are invalid
func (r *replicatorProps) isReplicatedTo(object *metav1.ObjectMeta, targetObject *metav1.ObjectMeta) (bool, error) {
	targets, targetPatterns, err := r.getReplicationTargets(object)
	if err != nil {
		return false, err
	}

	key := fmt.Sprintf("%s/%s", targetObject.Namespace, targetObject.Name)
	for _, t := range targets {
		if t == key {
			return true, nil
		}
	}

	for _, p := range targetPatterns {
		if p.Match(targetObject) {
			return true, nil
		}
	}

	return false, nil

	// return false, fmt.Error("source %s/%s is not replated to %s",
	// 	object.Namespace, object.Name, key)
}

// Returns everything needed to compute the desired targets
// - targets: a slice of all fully qualified target. Items are unique, does not contain object itself
// - targetPatterns: a slice of targetPattern, using regex to identify if a namespace is matched
//                   two patterns can generate the same target, and even the object itself
func (r *replicatorProps) getReplicationTargets(object *metav1.ObjectMeta) ([]string, []targetPattern, error) {
	annotationTo, okTo := object.Annotations[ReplicateToAnnotation]
	annotationToNs, okToNs := object.Annotations[ReplicateToNamespacesAnnotation]
	if !okTo && !okToNs {
		return nil, nil, nil
	}

	key := fmt.Sprintf("%s/%s", object.Name, object.Namespace)
	targets := []string{}
	targetPatterns := []targetPattern{}
	// cache of patterns, to reuse them as much as possible
	compiledPatterns := map[string]*regexp.Regexp{}
	for _, pattern := range r.watchedPatterns[key] {
		compiledPatterns[pattern.namespace.String()] = pattern.namespace
	}
	// which qualified paths have already been seen (exclude the object itself)
	seen := map[string]bool{key: true}
	var names, namespaces, qualified map[string]bool
	// no target explecitely provided, assumed that targets will have the same name
	if !okTo {
		names = map[string]bool{object.Name: true}
	// split the targets, and check which one are qualified
	} else {
		names = map[string]bool{}
		qualified = map[string]bool{}
		for _, n := range strings.Split(annotationTo, ",") {
			if n == "" {
			// a qualified name, with a namespace part
			} else if strings.ContainsAny(n, "/") {
				qualified[n] = true
			// a valid name
			} else if validName.MatchString(n) {
				names[n] = true
			// raise error
			} else {
				return nil, nil, fmt.Errorf("source %s has invalid name on annotation %s (%s)",
					key, ReplicateToAnnotation, n)
			}
		}
	}
	// no target namespace provided, assume that the namespace is the same (or qualified in the name)
	if !okToNs {
		namespaces = map[string]bool{object.Namespace: true}
	// split the target namespaces
	} else {
		namespaces = map[string]bool{}
		for _, ns := range strings.Split(annotationToNs, ",") {
			if strings.ContainsAny(ns, "/") {
				return nil, nil, fmt.Errorf("source %s has invalid namespace pattern on annotation %s (%s)",
					key, ReplicateToNamespacesAnnotation, ns)
			} else if ns != "" {
				namespaces[ns] = true
			}
		}
	}
	// join all the namespaces and names
	for ns := range namespaces {
		// this namespace is not a pattern
		if validName.MatchString(ns) {
			ns = ns + "/"
			for n := range names {
				full := ns + n
				if !seen[full] {
					seen[full] = true
					targets = append(targets, full)
				}
			}
		// this namespace is a pattern
		} else if pattern, err := regexp.Compile(`^(?:`+ns+`)$`); err == nil {
			compiledPatterns[ns] = pattern
			ns = ns + "/"
			for n := range names {
				full := ns + n
				if !seen[full] {
					seen[full] = true
					targetPatterns = append(targetPatterns, targetPattern{pattern, n})
				}
			}
		// raise compilation error
		} else {
			return nil, nil, fmt.Errorf("source %s has compilation error on annotation %s (%s): %s",
				key, ReplicateToNamespacesAnnotation, ns, err)
		}
	}
	// for all the qualified names, check if the namespace part is a pattern
	for q := range qualified {
		if seen[q] {
		// check that there is exactly one "/"
		} else if qs := strings.SplitN(q, "/", 3); len(qs) != 2 {
			return nil, nil, fmt.Errorf("source %s has invalid path on annotation %s (%s)",
				key, ReplicateToAnnotation, q)
		// check that the name part is valid
		} else if n := qs[1]; !validName.MatchString(n) {
			return nil, nil, fmt.Errorf("source %s has invalid name on annotation %s (%s)",
				key, ReplicateToAnnotation, n)
		// check if the namespace is a pattern
		} else if ns := qs[0]; validName.MatchString(ns) {
			targets = append(targets, q)
		// check if this pattern is already compiled
		} else if pattern, ok := compiledPatterns[ns]; ok {
			targetPatterns = append(targetPatterns, targetPattern{pattern, n})
		// check that the pattern compiles
		} else if pattern, err := regexp.Compile(`^(?:`+ns+`)$`); err == nil {
			compiledPatterns[ns] = pattern
			targetPatterns = append(targetPatterns, targetPattern{pattern, n})
		// raise compilation error
		} else {
			return nil, nil, fmt.Errorf("source %s has compilation error on annotation %s (%s): %s",
				key, ReplicateToAnnotation, ns, err)
		}
	}

	return targets, targetPatterns, nil
}

// Returns an annotation as "namespace/name" format
func resolveAnnotation(object *metav1.ObjectMeta, annotation string) (string, bool) {
	if val, ok := object.Annotations[annotation]; !ok {
		return "", false
	} else if strings.ContainsAny(val, "/") {
		return val, true
	} else {
		return fmt.Sprintf("%s/%s", object.Namespace, val), true
	}
}

// Returns true if the annotation from the object references the other object
func annotationRefersTo(object *metav1.ObjectMeta, annotation string, reference *metav1.ObjectMeta) bool {
	if val, ok := object.Annotations[annotation]; !ok {
		return false
	} else if v := strings.SplitN(val, "/", 2); len(v) == 2 {
		return v[0] == reference.Namespace && v[1] == reference.Name
	} else {
		return object.Namespace == reference.Namespace && val == reference.Name
	}
}
