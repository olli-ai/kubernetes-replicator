package replicate

import (
	"fmt"
	"testing"
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	fakev1 "k8s.io/client-go/kubernetes/typed/core/v1/fake"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This is dark magic to manage to do version checking on delete too
type SecretsFakeClient struct {
	fake.Clientset
}
func (c *SecretsFakeClient) CoreV1() corev1.CoreV1Interface {
	return &SecretsFakeCoreV1{fakev1.FakeCoreV1{Fake: &c.Fake}}
}
type SecretsFakeCoreV1 struct {
	fakev1.FakeCoreV1
}
func (c *SecretsFakeCoreV1) Secrets(namespace string) corev1.SecretInterface {
	return &SecretsFakeSecrets{*c.FakeCoreV1.Secrets(namespace).(*fakev1.FakeSecrets)}
}
var secretsResource = schema.GroupResource{Group: "", Resource: "secrets"}
type SecretsFakeSecrets struct {
	fakev1.FakeSecrets
}
func (c *SecretsFakeSecrets) Delete(name string, options *metav1.DeleteOptions) error {
	if options == nil {
	} else if pre := options.Preconditions; pre == nil {
	} else if ver := pre.ResourceVersion; ver == nil {
	} else if obj, err := c.Get(name, metav1.GetOptions{}); err != nil {
	} else if meta, err := GetMeta(obj); err != nil {
		return err
	} else if meta.ResourceVersion != *ver {
		return errors.NewConflict(secretsResource, name, fmt.Errorf(
				"has resource version \"%s\", but resource version \"%s\" provided",
				meta.ResourceVersion, *ver))
	}
	return c.FakeSecrets.Delete(name, options)
}

// Test that update and clear correctly manages the data
func TestSecrets_update_clear(t *testing.T) {
	client := &SecretsFakeClient{*fake.NewSimpleClientset()}
	AddResourceVersionReactor(&client.Clientset)
	repl := NewSecretReplicator(client, time.Hour, false)
	stop := repl.Start()
	defer stop()
	time.Sleep(SafeDuration)

	namespace := client.CoreV1().Namespaces()
	_, err := namespace.Create(&v1.Namespace {
		ObjectMeta: metav1.ObjectMeta {
			Name: "source-namespace",
		},
	})
	require.NoError(t, err)
	_, err = namespace.Create(&v1.Namespace {
		ObjectMeta: metav1.ObjectMeta {
			Name: "target-namespace",
		},
	})
	require.NoError(t, err)

	source, err := client.CoreV1().Secrets("source-namespace").Create(&v1.Secret {
		ObjectMeta: metav1.ObjectMeta {
			Name:        "source-name",
			Namespace:   "source-namespace",
			Annotations: map[string]string {
				ReplicationAllowed: "true",
			},
		},
		Data:       map[string][]byte {
			"source-data": []byte("true"),
			"data-field":  []byte("source-data"),
		},
	})
	require.NoError(t, err)

	target, err := client.CoreV1().Secrets("target-namespace").Create(&v1.Secret {
		ObjectMeta: metav1.ObjectMeta {
			Name:        "target-name",
			Namespace:   "target-namespace",
			Annotations: map[string]string {
				ReplicateFromAnnotation: "source-namespace/source-name",
			},
		},
		Data:       map[string][]byte {
			"target-data": []byte("true"),
			"data-field":  []byte("target-data"),
		},
	})
	require.NoError(t, err)

	time.Sleep(SafeDuration)
	target, err = client.CoreV1().Secrets("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Equal(t, source.Data, target.Data)
	}

	source = source.DeepCopy()
	source.Data = map[string][]byte {
		"other-data": []byte("true"),
		"data-field": []byte("other-data"),
	}
	source, err = client.CoreV1().Secrets("source-namespace").Update(source)

	time.Sleep(SafeDuration)
	target, err = client.CoreV1().Secrets("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Equal(t, source.Data, target.Data)
	}

	err = client.CoreV1().Secrets("source-namespace").Delete("source-name", &metav1.DeleteOptions{})
	require.NoError(t, err)

	time.Sleep(SafeDuration)
	target, err = client.CoreV1().Secrets("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Empty(t, target.Data)
	}
}

// Test that versionning works with update and clear
func TestSecrets_update_clear_version(t *testing.T) {
	client := &SecretsFakeClient{*fake.NewSimpleClientset()}
	AddResourceVersionReactor(&client.Clientset)
	repl := NewSecretReplicator(client, time.Hour, false).(*objectReplicator)

	namespace := client.CoreV1().Namespaces()
	ns, err := namespace.Create(&v1.Namespace {
		ObjectMeta: metav1.ObjectMeta {
			Name: "source-namespace",
		},
	})
	require.NoError(t, err)
	repl.namespaceStore.Update(ns)
	ns, err = namespace.Create(&v1.Namespace {
		ObjectMeta: metav1.ObjectMeta {
			Name: "target-namespace",
		},
	})
	require.NoError(t, err)
	repl.namespaceStore.Update(ns)

	// the replicator won't know about this placeholder, ensure that it cannot replace it
	placeholder, err := client.CoreV1().Secrets("target-namespace").Create(&v1.Secret {
		ObjectMeta: metav1.ObjectMeta {
			Name:        "target-name",
			Namespace:   "target-namespace",
		},
		Data:       map[string][]byte {
			"placeholder-data": []byte("true"),
			"data-field":       []byte("placeholder-data"),
		},
	})
	require.NoError(t, err)

	source := &v1.Secret {
		ObjectMeta: metav1.ObjectMeta {
			Name:        "source-name",
			Namespace:   "source-namespace",
			Annotations: map[string]string {
				ReplicationAllowed: "true",
			},
			ResourceVersion: "test10",
		},
		Data:       map[string][]byte {
			"source-data": []byte("true"),
			"data-field":  []byte("source-data"),
		},
	}
	repl.objectStore.Update(source)
	repl.ObjectAdded(source)

	target := &v1.Secret {
		ObjectMeta: metav1.ObjectMeta {
			Name:        "target-name",
			Namespace:   "target-namespace",
			Annotations: map[string]string {
				ReplicateFromAnnotation: "source-namespace/source-name",
			},
			ResourceVersion: "test20",
		},
		Data:       map[string][]byte {
			"target-data": []byte("true"),
			"data-field":  []byte("target-data"),
		},
	}
	repl.objectStore.Update(target)
	repl.ObjectAdded(target)
	target, err = client.CoreV1().Secrets("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Equal(t, placeholder.ResourceVersion, target.ResourceVersion)
		assert.Equal(t, placeholder.Data, target.Data)
	}

	target = &v1.Secret {
		ObjectMeta: metav1.ObjectMeta {
			Name:        "target-name",
			Namespace:   "target-namespace",
			Annotations: map[string]string {
				ReplicateFromAnnotation: "source-namespace/source-name",
				ReplicatedFromVersionAnnotation: "test40",
				ReplicatedAtAnnotation: "2000-01-01T00:00:00Z",
			},
			ResourceVersion: "test30",
		},
		Data:       map[string][]byte {
			"target-data": []byte("true"),
			"data-field":  []byte("target-data"),
		},
	}
	repl.objectStore.Update(target)
	repl.ObjectAdded(target)
	target, err = client.CoreV1().Secrets("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Equal(t, placeholder.ResourceVersion, target.ResourceVersion)
		assert.Equal(t, placeholder.Data, target.Data)
	}

	repl.objectStore.Delete(source)
	repl.ObjectDeleted(source)
	target, err = client.CoreV1().Secrets("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Equal(t, placeholder.ResourceVersion, target.ResourceVersion)
		assert.Equal(t, placeholder.Data, target.Data)
	}
}

// Test that install and delete correctly manages the data
func TestSecrets_install_delete(t *testing.T) {
	client := &SecretsFakeClient{*fake.NewSimpleClientset()}
	AddResourceVersionReactor(&client.Clientset)
	repl := NewSecretReplicator(client, time.Hour, false)
	stop := repl.Start()
	defer stop()
	time.Sleep(SafeDuration)

	namespace := client.CoreV1().Namespaces()
	_, err := namespace.Create(&v1.Namespace {
		ObjectMeta: metav1.ObjectMeta {
			Name: "source-namespace",
		},
	})
	require.NoError(t, err)
	_, err = namespace.Create(&v1.Namespace {
		ObjectMeta: metav1.ObjectMeta {
			Name: "target-namespace",
		},
	})
	require.NoError(t, err)

	source, err := client.CoreV1().Secrets("source-namespace").Create(&v1.Secret {
		ObjectMeta: metav1.ObjectMeta {
			Name:        "source-name",
			Namespace:   "source-namespace",
			Annotations: map[string]string {
				ReplicateToAnnotation: "target-namespace/target-name",
			},
		},
		Data:       map[string][]byte {
			"source-data": []byte("true"),
			"data-field":  []byte("source-data"),
		},
	})
	require.NoError(t, err)

	time.Sleep(SafeDuration)
	target, err := client.CoreV1().Secrets("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Equal(t, source.Data, target.Data)
	}

	source = source.DeepCopy()
	source.Data = map[string][]byte {
		"other-data": []byte("true"),
		"data-field": []byte("other-data"),
	}
	source, err = client.CoreV1().Secrets("source-namespace").Update(source)
	require.NoError(t, err)

	time.Sleep(SafeDuration)
	target, err = client.CoreV1().Secrets("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Equal(t, source.Data, target.Data)
	}

	err = client.CoreV1().Secrets("source-namespace").Delete("source-name", &metav1.DeleteOptions{})
	require.NoError(t, err)

	time.Sleep(SafeDuration)
	target, err = client.CoreV1().Secrets("target-namespace").Get("target-name", metav1.GetOptions{})
	if assert.Error(t, err) {
		require.IsType(t, &errors.StatusError{}, err)
		status := err.(*errors.StatusError)
		require.Equal(t, metav1.StatusReasonNotFound, status.ErrStatus.Reason)
	}
	assert.Nil(t, target)
}

// Test that versionning works with install and delete
func TestSecrets_install_delete_version(t *testing.T) {
	client := &SecretsFakeClient{*fake.NewSimpleClientset()}
	AddResourceVersionReactor(&client.Clientset)
	repl := NewSecretReplicator(client, time.Hour, false).(*objectReplicator)

	namespace := client.CoreV1().Namespaces()
	ns, err := namespace.Create(&v1.Namespace {
		ObjectMeta: metav1.ObjectMeta {
			Name: "source-namespace",
		},
	})
	require.NoError(t, err)
	repl.namespaceStore.Update(ns)
	ns, err = namespace.Create(&v1.Namespace {
		ObjectMeta: metav1.ObjectMeta {
			Name: "target-namespace",
		},
	})
	require.NoError(t, err)
	repl.namespaceStore.Update(ns)

	// the replicator won't know about this placeholder, ensure that it cannot replace it
	placeholder, err := client.CoreV1().Secrets("target-namespace").Create(&v1.Secret {
		ObjectMeta: metav1.ObjectMeta {
			Name:        "target-name",
			Namespace:   "target-namespace",
		},
		Data:       map[string][]byte {
			"placeholder-data": []byte("true"),
			"data-field":       []byte("placeholder-data"),
		},
	})
	require.NoError(t, err)

	source := &v1.Secret {
		ObjectMeta: metav1.ObjectMeta {
			Name:        "source-name",
			Namespace:   "source-namespace",
			Annotations: map[string]string {
				ReplicateToAnnotation: "target-namespace/target-name",
			},
			ResourceVersion: "test10",
		},
		Data:       map[string][]byte {
			"source-data": []byte("true"),
			"data-field":  []byte("source-data"),
		},
	}
	repl.objectStore.Update(source)
	repl.ObjectAdded(source)
	target, err := client.CoreV1().Secrets("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Equal(t, placeholder.ResourceVersion, target.ResourceVersion)
		assert.Equal(t, placeholder.Data, target.Data)
	}

	target = &v1.Secret {
		ObjectMeta: metav1.ObjectMeta {
			Name:        "target-name",
			Namespace:   "target-namespace",
			Annotations: map[string]string {
				ReplicatedByAnnotation: "source-namespace/source-name",
				ReplicatedFromVersionAnnotation: "test30",
				ReplicatedAtAnnotation: "2000-01-01T00:00:00Z",
			},
			ResourceVersion: "test20",
		},
		Data:       map[string][]byte {
			"target-data": []byte("true"),
			"data-field":  []byte("target-data"),
		},
	}
	repl.objectStore.Update(target)
	repl.ObjectAdded(target)
	target, err = client.CoreV1().Secrets("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Equal(t, placeholder.ResourceVersion, target.ResourceVersion)
		assert.Equal(t, placeholder.Data, target.Data)
	}

	repl.objectStore.Delete(source)
	repl.ObjectDeleted(source)
	target, err = client.CoreV1().Secrets("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Equal(t, placeholder.ResourceVersion, target.ResourceVersion)
		assert.Equal(t, placeholder.Data, target.Data)
	}
}