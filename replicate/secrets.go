package replicate

import (
	"time"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var SecretActions *secretActions = &secretActions{}

// NewSecretReplicator creates a new secret replicator
func NewSecretReplicator(client kubernetes.Interface, resyncPeriod time.Duration, allowAll bool) Replicator {
	repl := &objectReplicator{
		replicatorProps: replicatorProps{
			Name:            "secret",
			allowAll:        allowAll,
			client:          client,
		},
		replicatorActions: SecretActions,
	}
	repl.Init(resyncPeriod, client.CoreV1().Secrets(""), &v1.Secret{})
	return repl
}

type secretActions struct {}

func (*secretActions) getMeta(object interface{}) *metav1.ObjectMeta {
	return &object.(*v1.Secret).ObjectMeta
}

func (*secretActions) update(r *replicatorProps, object interface{}, sourceObject interface{}, annotations map[string]string) (interface{}, error) {
	sourceSecret := sourceObject.(*v1.Secret)
	secret := object.(*v1.Secret).DeepCopy()
	secret.Annotations = annotations

	if sourceSecret.Data != nil {
		secret.Data = make(map[string][]byte)
		for key, value := range sourceSecret.Data {
			newValue := make([]byte, len(value))
			copy(newValue, value)
			secret.Data[key] = newValue
		}
	} else {
		secret.Data = nil
	}

	return r.client.CoreV1().Secrets(secret.Namespace).Update(secret)
}

func (*secretActions) clear(r *replicatorProps, object interface{}, annotations map[string]string) (interface{}, error) {
	secret := object.(*v1.Secret).DeepCopy()
	secret.Data = nil
	secret.Annotations = annotations

	return r.client.CoreV1().Secrets(secret.Namespace).Update(secret)
}

func (*secretActions) install(r *replicatorProps, meta *metav1.ObjectMeta, sourceObject interface{}, dataObject interface{}) (interface{}, error) {
	sourceSecret := sourceObject.(*v1.Secret)
	secret := v1.Secret{
		Type: sourceSecret.Type,
		TypeMeta: metav1.TypeMeta{
			Kind:       sourceSecret.Kind,
			APIVersion: sourceSecret.APIVersion,
		},
		ObjectMeta: *meta,
	}

	if dataObject != nil {
		dataSecret := dataObject.(*v1.Secret)

		if dataSecret.Data != nil {
			secret.Data = make(map[string][]byte)
			for key, value := range dataSecret.Data {
				newValue := make([]byte, len(value))
				copy(newValue, value)
				secret.Data[key] = newValue
			}
		}
	}

	if secret.ResourceVersion == "" {
		return r.client.CoreV1().Secrets(secret.Namespace).Create(&secret)
	} else {
		return r.client.CoreV1().Secrets(secret.Namespace).Update(&secret)
	}
}

func (*secretActions) delete(r *replicatorProps, object interface{}) error {
	secret := object.(*v1.Secret)

	options := metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{
			ResourceVersion: &secret.ResourceVersion,
		},
	}

	return r.client.CoreV1().Secrets(secret.Namespace).Delete(secret.Name, &options)
}
