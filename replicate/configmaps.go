package replicate

import (
	"log"
	"time"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var ConfigMapActions *configMapActions = &configMapActions{}

// NewConfigMapReplicator creates a new config map replicator
func NewConfigMapReplicator(client kubernetes.Interface, resyncPeriod time.Duration, allowAll bool) Replicator {
	repl := objectReplicator{
		replicatorProps: replicatorProps{
			Name:            "config map",
			allowAll:        allowAll,
			client:          client,
		},
		replicatorActions: ConfigMapActions,
	}
	repl.Init(resyncPeriod, client.CoreV1().ConfigMaps(""), &v1.ConfigMap{})
	return &repl
}

type configMapActions struct {}

func (*configMapActions) getMeta(object interface{}) *metav1.ObjectMeta {
	return &object.(*v1.ConfigMap).ObjectMeta
}

func (*configMapActions) update(r *replicatorProps, object interface{}, sourceObject interface{}, annotations map[string]string) (interface{}, error) {
	sourceConfigMap := sourceObject.(*v1.ConfigMap)
	configMap := object.(*v1.ConfigMap).DeepCopy()
	configMap.Annotations = annotations

	if sourceConfigMap.Data != nil {
		configMap.Data = make(map[string]string)
		for key, value := range sourceConfigMap.Data {
			configMap.Data[key] = value
		}
	} else {
		configMap.Data = nil
	}

	if sourceConfigMap.BinaryData != nil {
		configMap.BinaryData = make(map[string][]byte)
		for key, value := range sourceConfigMap.BinaryData {
			newValue := make([]byte, len(value))
			copy(newValue, value)
			configMap.BinaryData[key] = newValue
		}
	} else {
		configMap.BinaryData = nil
	}

	log.Printf("updating config map %s/%s", configMap.Namespace, configMap.Name)

	return r.client.CoreV1().ConfigMaps(configMap.Namespace).Update(configMap)
}

func (*configMapActions) clear(r *replicatorProps, object interface{}, annotations map[string]string) (interface{}, error) {
	configMap := object.(*v1.ConfigMap).DeepCopy()
	configMap.Data = nil
	configMap.BinaryData = nil
	configMap.Annotations = annotations

	log.Printf("clearing config map %s/%s", configMap.Namespace, configMap.Name)

	return r.client.CoreV1().ConfigMaps(configMap.Namespace).Update(configMap)
}

func (*configMapActions) install(r *replicatorProps, meta *metav1.ObjectMeta, sourceObject interface{}, dataObject interface{}) (interface{}, error) {
	sourceConfigMap := sourceObject.(*v1.ConfigMap)
	configMap := v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       sourceConfigMap.Kind,
			APIVersion: sourceConfigMap.APIVersion,
		},
		ObjectMeta: *meta,
	}

	if dataObject != nil {
		dataConfigMap := dataObject.(*v1.ConfigMap)

		if dataConfigMap.Data != nil {
			configMap.Data = make(map[string]string)
			for key, value := range dataConfigMap.Data {
				configMap.Data[key] = value
			}
		}

		if dataConfigMap.BinaryData != nil {
			configMap.BinaryData = make(map[string][]byte)
			for key, value := range dataConfigMap.BinaryData {
				newValue := make([]byte, len(value))
				copy(newValue, value)
				configMap.BinaryData[key] = newValue
			}
		}
	}

	log.Printf("installing config map %s/%s", configMap.Namespace, configMap.Name)

	var s *v1.ConfigMap
	var err error
	if configMap.ResourceVersion == "" {
		return r.client.CoreV1().ConfigMaps(configMap.Namespace).Create(&configMap)
	} else {
		return r.client.CoreV1().ConfigMaps(configMap.Namespace).Update(&configMap)
	}
}

func (*configMapActions) delete(r *replicatorProps, object interface{}) error {
	configMap := object.(*v1.ConfigMap)
	log.Printf("deleting config map %s/%s", configMap.Namespace, configMap.Name)

	options := metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{
			ResourceVersion: &configMap.ResourceVersion,
		},
	}

	return r.client.CoreV1().ConfigMaps(configMap.Namespace).Delete(configMap.Name, &options)
}
