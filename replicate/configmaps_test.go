package replicate

import (
	"testing"
	"time"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_update_clear(t *testing.T) {
	client := fake.NewSimpleClientset()
	duration, err := time.ParseDuration("1h")
	require.NoError(t, err)
	repl := NewConfigMapReplicator(client, duration, false)
	stop := repl.Start()
	defer stop()

	namespace := client.CoreV1().Namespaces()
	_, err = namespace.Create(&v1.Namespace {
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

	source, err := client.CoreV1().ConfigMaps("source-namespace").Create(&v1.ConfigMap {
		ObjectMeta: metav1.ObjectMeta {
			Name:        "source-name",
			Namespace:   "source-namespace",
			Annotations: map[string]string {
				ReplicationAllowed: "true",
			},
		},
		Data:       map[string]string {
			"source-data": "true",
			"data-field":  "source-data",
		},
		BinaryData: map[string][]byte {
			"source-binary": []byte("true"),
			"binary-field":  []byte("source-binary"),
		},
	})
	require.NoError(t, err)

	_, err = client.CoreV1().ConfigMaps("target-namespace").Create(&v1.ConfigMap {
		ObjectMeta: metav1.ObjectMeta {
			Name:        "target-name",
			Namespace:   "target-namespace",
			Annotations: map[string]string {
				ReplicationAllowed: "true",
			},
		},
		Data:       map[string]string {
			"target-data": "true",
			"data-field":  "target-data",
		},
		BinaryData: map[string][]byte {
			"target-binary": []byte("true"),
			"binary-field":  []byte("target-binary"),
		},
	})
	require.NoError(t, err)

	target, err := client.CoreV1().ConfigMaps("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Equal(t, source.Data, target.Data)
		assert.Equal(t, source.BinaryData, target.BinaryData)
	}

	err = client.CoreV1().ConfigMaps("source-namespace").Delete("source-name", &metav1.DeleteOptions{})
	require.NoError(t, err)

	target, err = client.CoreV1().ConfigMaps("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Empty(t, target.Data)
		assert.Empty(t, target.BinaryData)
	}
}

func Test_install_delete(t *testing.T) {
	client := fake.NewSimpleClientset()
	duration, err := time.ParseDuration("1h")
	require.NoError(t, err)
	repl := NewConfigMapReplicator(client, duration, false)
	stop := repl.Start()
	defer stop()

	namespace := client.CoreV1().Namespaces()
	_, err = namespace.Create(&v1.Namespace {
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

	source, err := client.CoreV1().ConfigMaps("source-namespace").Create(&v1.ConfigMap {
		ObjectMeta: metav1.ObjectMeta {
			Name:        "source-name",
			Namespace:   "source-namespace",
			Annotations: map[string]string {
				ReplicateToAnnotation: "target-namespace/target-name",
			},
		},
		Data:       map[string]string {
			"source-data": "true",
			"data-field":  "source-data",
		},
		BinaryData: map[string][]byte {
			"source-binary": []byte("true"),
			"binary-field":  []byte("source-binary"),
		},
	})
	require.NoError(t, err)

	target, err := client.CoreV1().ConfigMaps("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Equal(t, source.Data, target.Data)
		assert.Equal(t, source.BinaryData, target.BinaryData)
	}

	source.Data = map[string]string {
		"other-data": "true",
		"data-field": "other-data",
	}
	source.BinaryData = map[string][]byte {
		"other-binary": []byte("true"),
		"binary-field": []byte("other-binary"),
	}
	source, err = client.CoreV1().ConfigMaps("source-namespace").Update(source)

	target, err = client.CoreV1().ConfigMaps("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	if assert.NotNil(t, target) {
		assert.Equal(t, source.Data, target.Data)
		assert.Equal(t, source.BinaryData, target.BinaryData)
	}

	err = client.CoreV1().ConfigMaps("source-namespace").Delete("source-name", &metav1.DeleteOptions{})
	require.NoError(t, err)

	target, err = client.CoreV1().ConfigMaps("target-namespace").Get("target-name", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Nil(t, target)
}
