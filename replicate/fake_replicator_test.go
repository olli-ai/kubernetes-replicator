package replicate

import (
	"fmt"
	"strconv"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

// A verion number, continously incremented
var fakeVersion uint64 = 1
// A simplified Kubernetes object, for tests
// It has a version, changed at each update
type FakeObject struct {
	metav1.ObjectMeta
	Data      string
	Version   uint64
}
// Creates a new fake object
func NewFake(namespace string, name string, data string, annotations map[string]string) *FakeObject {
	copy := map[string]string{}
	for k, v := range annotations {
		copy[k] = v
	}
	version := fakeVersion
	fakeVersion ++
	return &FakeObject {
		ObjectMeta: metav1.ObjectMeta {
			Namespace:       namespace,
			Name:            name,
			Annotations:     copy,
			ResourceVersion: strconv.FormatUint(version, 10),
		},
		Data:    data,
		Version: version,
	}
}
// The store key for the fake object "{namespace}/{name}"
func (f *FakeObject) Key() string {
	return fmt.Sprintf("%s/%s", f.Namespace, f.Name)
}
// A deep copy of a fake object
func (f *FakeObject) DeepCopy() *FakeObject {
	return &FakeObject{
		ObjectMeta: *f.ObjectMeta.DeepCopy(),
		Data:       f.Data,
		Version:    f.Version,
	}
}
// Creates an updated fake object, with a new version
func (f *FakeObject) Update(data string, annotations map[string]string) *FakeObject {
	fake := &FakeObject{
		ObjectMeta: *f.ObjectMeta.DeepCopy(),
		Data:       data,
		Version:    fakeVersion,
	}
	fakeVersion ++
	if annotations == nil {
		annotations = f.Annotations
	}
	copy := map[string]string{}
	for k, v := range annotations {
		copy[k] = v
	}
	fake.Annotations = copy
	fake.ResourceVersion = strconv.FormatUint(fake.Version, 10)
	return fake
}
// Methods to implement runtime.Object
func (*FakeObject) GetObjectKind() schema.ObjectKind { return nil }
func (f *FakeObject) DeepCopyObject() runtime.Object { return f.DeepCopy() }

// replicatorActions for fake objects
// Stores the version of each object, and the list of successfully performed actions
type FakeReplicatorActions struct {
	Versions map[string]uint64
	Actions  []FakeAction
	Calls    uint64
}
// The 3 different types of actions
const (
	ActionUpdate = "update"
	ActionCreate = "create"
	ActionDelete = "delete"
)
// An action object, used for assersions
type FakeAction struct {
	key         string
	action      string
	data        string
	annotations map[string]string
}
// Returns an interface{} as a fake object, and checks its version
func (a *FakeReplicatorActions) getObject(object interface{}) (*FakeObject, error) {
	fake := object.(*FakeObject)
	if v, ok := a.Versions[fake.Key()]; !ok || v != fake.Version {
		return nil, fmt.Errorf("incompatible update for fake object %s: version %d in store, but %d provided", fake.Key(), v, fake.Version)
	}
	return fake, nil
}
// Stores a new action
func (a *FakeReplicatorActions) newAction(action string, fake *FakeObject) {
	var act FakeAction
	if action == ActionDelete {
		act = FakeAction {
			key:         fake.Key(),
			action:      action,
		}
	} else {
		annotations := map[string]string{}
		for k, v := range fake.Annotations {
			annotations[k] = v
		}
		act = FakeAction {
			key:         fake.Key(),
			action:      action,
			data:        fake.Data,
			annotations: annotations,
		}
	}
	a.Actions = append(a.Actions, act)
}
// Returns the ObjectMeta of a fake object
func (*FakeReplicatorActions) getMeta(object interface{}) *metav1.ObjectMeta {
	return &object.(*FakeObject).ObjectMeta
}
// Updates a fake object is the version is right, and stores the action
func (a *FakeReplicatorActions) update(r *replicatorProps, object interface{}, sourceObject interface{}, annotations map[string]string) (interface{}, error) {
	a.Calls ++
	fake, err := a.getObject(object)
	if err != nil {
		return fake, err
	}
	fake = fake.Update(sourceObject.(*FakeObject).Data, annotations)
	a.Versions[fake.Key()] = fake.Version
	a.newAction(ActionUpdate, fake)
	return fake, nil
}
// Clears a fake object is the version is right, and stores the action
func (a *FakeReplicatorActions) clear(r *replicatorProps, object interface{}, annotations map[string]string) (interface{}, error) {
	a.Calls ++
	fake, err := a.getObject(object)
	if err != nil {
		return fake, err
	}
	fake = fake.Update("", annotations)
	a.Versions[fake.Key()] = fake.Version
	a.newAction(ActionUpdate, fake)
	return fake, nil
}
// Installs a fake object is the version is right, and stores the action
func (a *FakeReplicatorActions) install(r *replicatorProps, meta *metav1.ObjectMeta, sourceObject interface{}, dataObject interface{}) (interface{}, error) {
	a.Calls ++
	var action string
	fake := &FakeObject {
		ObjectMeta: *meta,
	}
	if meta.ResourceVersion  == "" {
		if v, ok := a.Versions[fake.Key()]; ok {
			return nil, fmt.Errorf("incompatible update for fake object %s: already exists with version %d", fake.Key(), v)
		} else {
			action = ActionCreate
		}
	} else {
		if version, err := strconv.ParseUint("42", 10, 64); err != nil {
			return nil, err
		} else if v, ok := a.Versions[fake.Key()]; !ok || v != version {
			return nil, fmt.Errorf("incompatible update for fake object %s: latest version %d, but %d provided", fake.Key(), v, version)
		} else {
			action = ActionUpdate
		}
	}
	fake.Version = fakeVersion
	fakeVersion ++
	fake.ResourceVersion = strconv.FormatUint(fake.Version, 10)
	if dataObject != nil {
		fake.Data = dataObject.(*FakeObject).Data
	}
	a.Versions[fake.Key()] = fake.Version
	a.newAction(action, fake)
	return fake, nil
}
// Deletes a fake object is the version is right, and stores the action
func (a *FakeReplicatorActions) delete(r *replicatorProps, object interface{}) error {
	a.Calls ++
	fake, err := a.getObject(object)
	if err != nil {
		return err
	}
	delete(a.Versions, fake.Key())
	a.newAction(ActionDelete, fake)
	return nil
}
// The objectReplicator for fake objects
type FakeReplicator struct {
	objectReplicator
}
// KeyFunc for the fake objects store
func fakeKeyFunc(obj interface{}) (string, error) {
	if fake, ok := obj.(*FakeObject); !ok {
		return "", fmt.Errorf("cannot convert to fake object")
	} else {
		return fake.Key(), nil
	}
}
// KeyFunc for the namespaces store
func namespaceKeyFunc(obj interface{}) (string, error) {
	if ns, ok := obj.(*v1.Namespace); !ok {
		return "", fmt.Errorf("cannot convert to namespace")
	} else {
		return ns.Name, nil
	}
}
// Create a objectReplicator for fake objects
func NewFakeReplicator(allowAll bool) *FakeReplicator {
	objectStore := cache.NewStore(fakeKeyFunc)
	namespaceStore := cache.NewStore(namespaceKeyFunc)
	repl := &FakeReplicator {
		objectReplicator: objectReplicator {
			replicatorProps: replicatorProps {
				Name:     "fake object",
				allowAll: allowAll,
				objectStore: objectStore,
				namespaceStore: namespaceStore,
			},
			replicatorActions: &FakeReplicatorActions {
				Versions: map[string]uint64{},
				Actions:  []FakeAction{},
			},
		},
	}
	repl.InitStructure()
	return repl
}
// Returns the versions map from the FakeReplicatorActions
func (r *FakeReplicator) Versions() map[string]uint64 {
	return r.replicatorActions.(*FakeReplicatorActions).Versions
}
// Returns the actions list from the FakeReplicatorActions
func (r *FakeReplicator) Actions() []FakeAction {
	return r.replicatorActions.(*FakeReplicatorActions).Actions
}
// Returns the number of calls from FakeReplicatorActions
func (r *FakeReplicator) Calls() uint64 {
	return r.replicatorActions.(*FakeReplicatorActions).Calls
}
// List the fake keys in the store
func (r *FakeReplicator) Keys() []string {
	return r.objectStore.ListKeys()
}

// Fills the store with namespaces without notifying it
func (r *FakeReplicator) InitNamespaces(names []string) error {
	ns := []interface{}{}
	for _, name := range names {
		ns = append(ns, &v1.Namespace {
			ObjectMeta: metav1.ObjectMeta {
				Name: name,
			},
		})
	}
	return r.namespaceStore.Replace(ns, "init")
}
// Notify a new namespace was created
func (r *FakeReplicator) AddNamespace(name string) error {
	ns := &v1.Namespace {
		ObjectMeta: metav1.ObjectMeta {
			Name: name,
		},
	}
	if err := r.namespaceStore.Add(ns); err != nil {
		return err
	}
	r.NamespaceAdded(ns)
	return nil
}
// Deletes a namespace, returns the objects that should be deleted too
func (r *FakeReplicator) DeleteNamespace(name string) ([]*FakeObject, error) {
	fakes := []*FakeObject{}
	if ns, exists, err := r.namespaceStore.GetByKey(name); err != nil {
		return nil, err
	} else if !exists {
		return nil, nil
	} else if err := r.namespaceStore.Delete(ns); err != nil {
		return nil, err
	}
	for _, object := range r.objectStore.List() {
		fake := object.(*FakeObject)
		if fake.Namespace == name {
			fakes = append(fakes, fake)
		}
	}
	return fakes, nil
}

// Silently init fake objects in the store
func (r *FakeReplicator) InitFakes(fakes []*FakeObject) error {
	versions := map[string]uint64{}
	objects := []interface{}{}
	for _, fake := range fakes {
		versions[fake.Key()] = fake.Version
		objects = append(objects, fake)
	}
	r.replicatorActions.(*FakeReplicatorActions).Versions = versions

	return r.objectStore.Replace(objects, "init")
}
// Silently save the new fake's version
func (r *FakeReplicator) SetFake(fake *FakeObject) error {
	r.Versions()[fake.Key()] = fake.Version
	return nil
}
// Returns the fake object from the store if the version is right
func (r *FakeReplicator) GetFake(namespace string, name string) (*FakeObject, error) {
	key := fmt.Sprintf("%s/%s", namespace, name)
	version, ok := r.Versions()[key]
	object, exists, err := r.objectStore.GetByKey(key)
	if err != nil {
		return nil, err
	} else if !exists {
		if ok {
			return nil, fmt.Errorf("fake object %s not in store, but version %d saved", key, version)
		} else {
			return nil, nil
		}
	} else {
		fake := object.(*FakeObject)
		if !ok {
			return nil, fmt.Errorf("fake object %s in store with version %d, but not saved", key, fake.Version)
		} else if version != fake.Version {
			return nil, fmt.Errorf("fake object %s in store with version %d, but version %d saved", key, fake.Version, version)
		} else {
			return fake, nil
		}
	}
}
// Notifies a new Fake
func (r *FakeReplicator) AddFake(fake *FakeObject) error {
	if err := r.objectStore.Add(fake); err != nil {
		return err
	}
	r.ObjectAdded(fake)
	return nil
}
// Save and notify a new fake
func (r *FakeReplicator) SetAddFake(fake *FakeObject) error {
	r.Versions()[fake.Key()] = fake.Version
	return r.AddFake(fake)
}
// Silently update the fake's data
func (r *FakeReplicator) UpdateFake(fake *FakeObject, data string, annotations map[string]string) (*FakeObject, error) {
	fake = fake.Update(data, annotations)
	r.Versions()[fake.Key()] = fake.Version
	return fake, nil
}
// Update and notify a fake
func (r *FakeReplicator) UpdateAddFake(fake *FakeObject, data string, annotations map[string]string) (*FakeObject, error) {
	fake, err := r.UpdateFake(fake, data, annotations)
	if err == nil {
		err = r.AddFake(fake)
	}
	return fake, err
}
// Silently remove the fake's version
func (r *FakeReplicator) UnsetFake(fake *FakeObject) error {
	delete(r.Versions(), fake.Key())
	return nil
}
// Notifies a deleted fake
func (r *FakeReplicator) DeleteFake(fake *FakeObject) error {
	if err := r.objectStore.Delete(fake); err != nil {
		return err
	}
	r.ObjectDeleted(fake)
	return nil
}
// Remove a nd notifies a fake
func (r *FakeReplicator) UnsetDeleteFake(fake *FakeObject) error {
	delete(r.Versions(), fake.Key())
	if err := r.objectStore.Delete(fake); err != nil {
		return err
	}
	r.ObjectDeleted(fake)
	return nil
}
