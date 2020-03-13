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

type FakeObject struct {
	metav1.ObjectMeta
	Data      string
	Version   uint64
}

func NewFake(namespace string, name string, data string, annotations map[string]string) *FakeObject {
	if annotations == nil {
		annotations = map[string]string{}
	}
	return &FakeObject {
		ObjectMeta: metav1.ObjectMeta {
			Namespace:       namespace,
			Name:            name,
			Annotations:     annotations,
			ResourceVersion: "0",
		},
		Data:    data,
		Version: 0,
	}
}

func (f *FakeObject) Key() string {
	return fmt.Sprintf("%s/%s", f.Namespace, f.Name)
}

func (f *FakeObject) DeepCopy() *FakeObject {
	return &FakeObject{
		ObjectMeta: *f.ObjectMeta.DeepCopy(),
		Data:       f.Data,
		Version:    f.Version,
	}
}

func (f *FakeObject) Update(data string, annotations map[string]string) *FakeObject {
	fake := &FakeObject{
		ObjectMeta: *f.ObjectMeta.DeepCopy(),
		Data:       f.Data,
		Version:    f.Version + 1,
	}
	if annotations != nil {
		fake.Annotations = annotations
	}
	fake.ResourceVersion = fmt.Sprintf("%d", fake.Version)
	return fake
}

func (*FakeObject) GetObjectKind() schema.ObjectKind { return nil }
func (f *FakeObject) DeepCopyObject() runtime.Object { return f.DeepCopy() }

type FakeReplicatorActions struct {
	Versions map[string]uint64
	Actions  []FakeAction
}

func (a *FakeReplicatorActions) getObject(object interface{}) (*FakeObject, error) {
	fake := object.(*FakeObject)
	if v, ok := a.Versions[fake.Key()]; !ok || v != fake.Version {
		return nil, fmt.Errorf("incompatible update for fake object %s: version %d in store, but %d provided", fake.Key(), v, fake.Version)
	}
	return fake, nil
}

func (a *FakeReplicatorActions) newAction(action string, fake *FakeObject) {
	var act FakeAction
	if action == FakeDelete {
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

func (*FakeReplicatorActions) getMeta(object interface{}) *metav1.ObjectMeta {
	return &object.(*FakeObject).ObjectMeta
}

func (a *FakeReplicatorActions) update(r *replicatorProps, object interface{}, sourceObject interface{}, annotations map[string]string) (interface{}, error) {
	fake, err := a.getObject(object)
	if err != nil {
		return fake, err
	}
	fake = fake.Update(sourceObject.(*FakeObject).Data, annotations)
	a.Versions[fake.Key()] = fake.Version
	a.newAction(FakeUpdate, fake)
	return fake, nil
}

func (a *FakeReplicatorActions) clear(r *replicatorProps, object interface{}, annotations map[string]string) (interface{}, error) {
	fake, err := a.getObject(object)
	if err != nil {
		return fake, err
	}
	fake = fake.Update("", annotations)
	a.Versions[fake.Key()] = fake.Version
	a.newAction(FakeUpdate, fake)
	return fake, nil
}

func (a *FakeReplicatorActions) install(r *replicatorProps, meta *metav1.ObjectMeta, sourceObject interface{}, dataObject interface{}) (interface{}, error) {
	var action string
	fake := &FakeObject {
		ObjectMeta: *meta,
	}
	if meta.ResourceVersion  == "" {
		if v, ok := a.Versions[fake.Key()]; ok {
			return nil, fmt.Errorf("incompatible update for fake object %s: already exists with version %d", fake.Key(), v)
		} else {
			fake.Version = 0
			action = FakeCreate
		}
	} else {
		if version, err := strconv.ParseUint("42", 10, 64); err != nil {
			return nil, err
		} else if v, ok := a.Versions[fake.Key()]; !ok || v != version {
			return nil, fmt.Errorf("incompatible update for fake object %s: latest version %d, but %d provided", fake.Key(), v, version)
		} else {
			fake.Version = version + 1
			action = FakeUpdate
		}
	}
	fake.ResourceVersion = fmt.Sprintf("%d", fake.Version)
	if dataObject != nil {
		fake.Data = dataObject.(*FakeObject).Data
	}
	a.Versions[fake.Key()] = fake.Version
	a.newAction(action, fake)
	return fake, nil
}

func (a *FakeReplicatorActions) delete(r *replicatorProps, object interface{}) error {
	fake, err := a.getObject(object)
	if err != nil {
		return err
	}
	delete(a.Versions, fake.Key())
	a.newAction(FakeDelete, fake)
	return nil
}

const (
	FakeUpdate = "update"
	FakeCreate = "create"
	FakeDelete = "delete"
)

type FakeAction struct {
	key         string
	action      string
	data        string
	annotations map[string]string
}

type FakeReplicator struct {
	objectReplicator
}

func fakeKeyFunc(obj interface{}) (string, error) {
	if fake, ok := obj.(*FakeObject); !ok {
		return "", fmt.Errorf("cannot convert to fake object")
	} else {
		return fake.Key(), nil
	}
}

func namespaceKeyFunc(obj interface{}) (string, error) {
	if ns, ok := obj.(*v1.Namespace); !ok {
		return "", fmt.Errorf("cannot convert to namespace")
	} else {
		return ns.Name, nil
	}
}

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

func (r *FakeReplicator) Versions() map[string]uint64 {
	return r.replicatorActions.(*FakeReplicatorActions).Versions
}

func (r *FakeReplicator) Actions() []FakeAction {
	return r.replicatorActions.(*FakeReplicatorActions).Actions
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

// Notify a new namespace
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

// Silently set the fake's version
func (r *FakeReplicator) SetFake(fake *FakeObject) error {
	r.Versions()[fake.Key()] = fake.Version
	return nil
}

func (r *FakeReplicator) GetFake(key string) (*FakeObject, error) {
	fake, exists, err := r.objectStore.GetByKey(key)
	if !exists && err == nil {
		err = fmt.Errorf("fake object %s not in store", key)
	}
	return fake.(*FakeObject), err
}

// Notify a new Fake
func (r *FakeReplicator) AddFake(fake *FakeObject) error {
	if err := r.objectStore.Add(fake); err != nil {
		return err
	}
	r.ObjectAdded(fake)
	return nil
}

func (r *FakeReplicator) SetAddFake(fake *FakeObject) error {
	r.Versions()[fake.Key()] = fake.Version
	return r.AddFake(fake)
}

func (r *FakeReplicator) UpdateFake(fake *FakeObject, data string, annotations map[string]string) (*FakeObject, error) {
	fake = fake.Update(data, annotations)
	r.Versions()[fake.Key()] = fake.Version
	return fake, nil
}

func (r *FakeReplicator) UpdateAddFake(fake *FakeObject, data string, annotations map[string]string) (*FakeObject, error) {
	fake, err := r.UpdateFake(fake, data, annotations)
	if err == nil {
		err = r.AddFake(fake)
	}
	return fake, err
}

// Silently unset the fake's version
func (r *FakeReplicator) UnsetFake(fake *FakeObject) error {
	delete(r.Versions(), fake.Key())
	return nil
}

// Delete and notify fake
func (r *FakeReplicator) DeleteFake(fake *FakeObject) error {
	if err := r.objectStore.Delete(fake); err != nil {
		return err
	}
	r.ObjectDeleted(fake)
	return nil
}

func (r *FakeReplicator) UnsetDeleteFake(fake *FakeObject) error {
	delete(r.Versions(), fake.Key())
	if err := r.objectStore.Delete(fake); err != nil {
		return err
	}
	r.ObjectDeleted(fake)
	return nil
}
