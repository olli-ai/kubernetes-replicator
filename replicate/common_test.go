package replicate

import (
	"sort"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stretchr/testify/assert"
)

func Test_isReplicationAllowedAnnotation(t *testing.T) {
	examples := [] struct {
		// name of the test
		name        string
		// if replication should be allowed
		allowed     bool
		// if replication is disallowed
		disallowed  bool
		// --allow-all global option
		allowAll    bool
		// target namespace
		namespace   string
		// source annotations
		annotations map[string]string
	}{{
		"--allow-all",
		true,
		false,
		true,
		"target-namespace",
		map[string]string{},
	},{
		"--allow-all but explicitely disallow",
		false,
		true,
		true,
		"target-namespace",
		map[string]string{ReplicationAllowedAnnotation: "false"},
	},{
		"--allow-all but restrict namespace",
		false,
		true,
		true,
		"target-namespace",
		map[string]string{AllowedNamespacesAnnotation: "other-namespace"},
	},{
		"--allow-all but restrict namespace with pattern",
		false,
		true,
		true,
		"target-namespace",
		map[string]string{AllowedNamespacesAnnotation: "other-.*"},
	},{
		"--allow-all but illformed annotation",
		false,
		false,
		true,
		"target-namespace",
		map[string]string{ReplicationAllowedAnnotation: "other"},
	},{
		"--allow-all but illformed namespaces annotation",
		false,
		false,
		true,
		"target-namespace",
		map[string]string{AllowedNamespacesAnnotation: "(other"},
	},{
		"--allow-all but from annotation",
		false,
		false,
		true,
		"target-namespace",
		map[string]string{ReplicationSourceAnnotation: "other-object"},
	},{
		"explicitely allow",
		true,
		false,
		false,
		"target-namespace",
		map[string]string{ReplicationAllowedAnnotation: "true"},
	},{
		"explicitely allow namespace",
		true,
		false,
		false,
		"target-namespace",
		map[string]string{AllowedNamespacesAnnotation: "target-namespace"},
	},{
		"explicitely allow namespace list",
		true,
		false,
		false,
		"target-namespace",
		map[string]string{AllowedNamespacesAnnotation: "first-namespace,target-namespace,second-namespace"},
	},{
		"explicitely allow namespace pattern",
		true,
		false,
		false,
		"target-namespace",
		map[string]string{AllowedNamespacesAnnotation: "target-.*"},
	},{
		"explicitely allow namespace pattern list",
		true,
		false,
		false,
		"target-namespace",
		map[string]string{AllowedNamespacesAnnotation: "first-.*,target-.*,second-.*"},
	}}
	for _, example := range examples {
		rep := &replicatorProps {
			Name:     "object",
			allowAll: example.allowAll,
		}
		target := &metav1.ObjectMeta {
			Name:      "target-object",
			Namespace: example.namespace,
		}
		source := &metav1.ObjectMeta {
			Name:        "source-object",
			Namespace:   "source-namespace",
			Annotations: example.annotations,
		}
		allowed, disallowed, err := rep.isReplicationAllowedAnnotation(target, source)
		if example.allowed {
			assert.True(t, allowed, example.name)
			assert.False(t, disallowed, example.name)
			assert.NoError(t, err, example.name)
		} else {
			assert.False(t, allowed, example.name)
			assert.Error(t, err, example.name)
		}
		if example.disallowed {
			assert.False(t, allowed, example.name)
			assert.True(t, disallowed, example.name)
			assert.Error(t, err, example.name)
		} else {
			assert.False(t, disallowed, example.name)
		}
	}
}

func Test_needsDataUpdate(t *testing.T) {
	examples := [] struct {
		// name of the test
		name    string
		// if update is needed
		needed  bool
		// if update is not needed because of "once"
		once    bool
		// the source annotations
		source  map[string]string
		// the source resource version
		version string
		// the target annotations
		target  map[string]string
	}{{
		"never replicated",
		true,
		false,
		map[string]string{},
		"1",
		map[string]string{},
	},{
		"right resource version",
		false,
		false,
		map[string]string{},
		"1",
		map[string]string{ReplicatedVersionAnnotation: "1"},
	},{
		"wrong resource version",
		true,
		false,
		map[string]string{},
		"2",
		map[string]string{ReplicatedVersionAnnotation: "1"},
	},{
		"replicate once (source), never replicated",
		true,
		false,
		map[string]string{ReplicateOnceAnnotation: "true"},
		"2",
		map[string]string{},
	},{
		"replicate once (source), wrong resource version",
		false,
		true,
		map[string]string{ReplicateOnceAnnotation: "true"},
		"2",
		map[string]string{ReplicatedVersionAnnotation: "1"},
	},{
		"replicate once (source), lower once version",
		true,
		false,
		map[string]string{
			ReplicateOnceAnnotation: "true",
			ReplicateOnceVersionAnnotation: "1.2.3",
		},
		"2",
		map[string]string{
			ReplicatedVersionAnnotation: "1",
			ReplicateOnceVersionAnnotation: "1.1.4",
		},
	},{
		"replicate once (source), same once version",
		false,
		true,
		map[string]string{
			ReplicateOnceAnnotation: "true",
			ReplicateOnceVersionAnnotation: "1.2.3",
		},
		"2",
		map[string]string{
			ReplicatedVersionAnnotation: "1",
			ReplicateOnceVersionAnnotation: "1.2.3",
		},
	},{
		"replicate once (source), higher once version",
		false,
		true,
		map[string]string{
			ReplicateOnceAnnotation: "true",
			ReplicateOnceVersionAnnotation: "1.2.3",
		},
		"2",
		map[string]string{
			ReplicatedVersionAnnotation: "1",
			ReplicateOnceVersionAnnotation: "1.3.2",
		},
	},{
		"replicate once (target), never replicated",
		true,
		false,
		map[string]string{},
		"2",
		map[string]string{ReplicateOnceAnnotation: "true"},
	},{
		"replicate once (target), wrong resource version",
		false,
		true,
		map[string]string{},
		"2",
		map[string]string{
			ReplicateOnceAnnotation: "true",
			ReplicatedVersionAnnotation: "1",
		},
	},{
		"replicate once (target), lower once version",
		true,
		false,
		map[string]string{
			ReplicateOnceVersionAnnotation: "1.2.3",
		},
		"2",
		map[string]string{
			ReplicateOnceAnnotation: "true",
			ReplicatedVersionAnnotation: "1",
			ReplicateOnceVersionAnnotation: "1.1.4",
		},
	},{
		"replicate once (target), same once version",
		false,
		true,
		map[string]string{
			ReplicateOnceVersionAnnotation: "1.2.3",
		},
		"2",
		map[string]string{
			ReplicateOnceAnnotation: "true",
			ReplicatedVersionAnnotation: "1",
			ReplicateOnceVersionAnnotation: "1.2.3",
		},
	},{
		"replicate once (target), higher once version",
		false,
		true,
		map[string]string{
			ReplicateOnceVersionAnnotation: "1.2.3",
		},
		"2",
		map[string]string{
			ReplicateOnceAnnotation: "true",
			ReplicatedVersionAnnotation: "1",
			ReplicateOnceVersionAnnotation: "1.3.2",
		},
	},{
		"replicate once, source but not target",
		false,
		true,
		map[string]string{
			ReplicateOnceAnnotation: "true",
		},
		"2",
		map[string]string{
			ReplicateOnceAnnotation: "false",
			ReplicatedVersionAnnotation: "1",
		},
	},{
		"replicate once, target but not source",
		false,
		true,
		map[string]string{
			ReplicateOnceAnnotation: "false",
		},
		"2",
		map[string]string{
			ReplicateOnceAnnotation: "true",
			ReplicatedVersionAnnotation: "1",
		},
	},{
		"illformed once annotation (source)",
		false,
		false,
		map[string]string{
			ReplicateOnceAnnotation: "other",
			ReplicateOnceVersionAnnotation: "1.1.1",
		},
		"2",
		map[string]string{
			ReplicatedVersionAnnotation: "1",
			ReplicateOnceVersionAnnotation: "2.2.2",
		},
	},{
		"illformed once annotation (target)",
		false,
		false,
		map[string]string{
			ReplicateOnceVersionAnnotation: "1.1.1",
		},
		"2",
		map[string]string{
			ReplicateOnceAnnotation: "other",
			ReplicatedVersionAnnotation: "1",
			ReplicateOnceVersionAnnotation: "2.2.2",
		},
	},{
		"illformed once annotation (source)",
		false,
		false,
		map[string]string{
			ReplicateOnceVersionAnnotation: "other",
		},
		"2",
		map[string]string{
			ReplicateOnceAnnotation: "true",
			ReplicatedVersionAnnotation: "1",
			ReplicateOnceVersionAnnotation: "2.2.2",
		},
	},{
		"illformed once annotation (target)",
		false,
		false,
		map[string]string{
			ReplicateOnceVersionAnnotation: "1.1.1",
			ReplicateOnceAnnotation: "true",
		},
		"2",
		map[string]string{
			ReplicatedVersionAnnotation: "1",
			ReplicateOnceVersionAnnotation: "other",
		},
	}}
	rep := &replicatorProps {
		Name:     "object",
	}
	for _, example := range examples {
		target := &metav1.ObjectMeta {
			Name:        "target-object",
			Namespace:   "target-namespace",
			Annotations: example.target,
		}
		source := &metav1.ObjectMeta {
			Name:            "source-object",
			Namespace:       "source-namespace",
			Annotations:     example.source,
			ResourceVersion: example.version,
		}
		needed, once, err := rep.needsDataUpdate(target, source)
		if example.needed {
			assert.True(t, needed, example.name)
			assert.False(t, once, example.name)
			assert.NoError(t, err, example.name)
		} else {
			assert.False(t, needed, example.name)
			assert.Error(t, err, example.name)
		}
		if once {
			assert.True(t, once, example.name)
		} else {
			assert.False(t, once, example.name)
		}
	}
}

func Test_needsFromAnnotationsUpdate(t *testing.T) {
	examples := [] struct {
		// the name of the test
		name   string
		// if update is needed
		needed bool
		// if error is expected
		err    bool
		// the source annotations
		source map[string]string
		// the target annotations
		target map[string]string
	}{{
		"same from annotation",
		false,
		false,
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
		},
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
		},
	},{
		"no from annotation",
		true,
		false,
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
		},
		map[string]string {},
	},{
		"no from annotation both",
		false,
		true,
		map[string]string {},
		map[string]string {},
	},{
		"different from annotation name",
		true,
		false,
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
		},
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/other-object",
		},
	},{
		"different from annotation namespace",
		true,
		false,
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
		},
		map[string]string {
			ReplicationSourceAnnotation: "other-namespace/data-object",
		},
	},{
		"same from annotation without namespace",
		false,
		false,
		map[string]string {
			ReplicationSourceAnnotation: "data-object",
		},
		map[string]string {
			ReplicationSourceAnnotation: "source-namespace/data-object",
		},
	},{
		"different from annotation without namespace",
		true,
		false,
		map[string]string {
			ReplicationSourceAnnotation: "data-object",
		},
		map[string]string {
			ReplicationSourceAnnotation: "other-namespace/data-object",
		},
	},{
		"illformed from annotation",
		false,
		true,
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object/other",
		},
		map[string]string {},
	},{
		"from annotation same as source",
		false,
		true,
		map[string]string {
			ReplicationSourceAnnotation: "source-namespace/source-object",
		},
		map[string]string {},
	},{
		"from annotation same as source without namespace",
		false,
		true,
		map[string]string {
			ReplicationSourceAnnotation: "source-object",
		},
		map[string]string {},
	},{
		"same once annotation",
		false,
		false,
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
			ReplicateOnceAnnotation: "true",
		},
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
			ReplicateOnceAnnotation: "true",
		},
	},{
		"no once annotation",
		true,
		false,
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
			ReplicateOnceAnnotation: "false",
		},
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
		},
	},{
		"different once annotation",
		true,
		false,
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
			ReplicateOnceAnnotation: "true",
		},
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
			ReplicateOnceAnnotation: "false",
		},
	},{
		"illformed once annotation",
		false,
		true,
		map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
			ReplicateOnceAnnotation: "other",
		},
		map[string]string {},
	}}
	rep := &replicatorProps {
		Name:     "object",
	}
	for _, example := range examples {
		target := &metav1.ObjectMeta {
			Name:        "target-object",
			Namespace:   "target-namespace",
			Labels:      getCopyLabels(),
			Annotations: example.target,
		}
		source := &metav1.ObjectMeta {
			Name:        "source-object",
			Namespace:   "source-namespace",
			Annotations: example.source,
		}
		needed, err := rep.needsFromAnnotationsUpdate(target, source)
		if example.needed {
			assert.True(t, needed, example.name)
			assert.NoError(t, err, example.name)
		} else {
			assert.False(t, needed, example.name)
		}
		if example.err {
			assert.Error(t, err, example.name)
		} else {
			assert.NoError(t, err, example.name)
		}
	}

	target := &metav1.ObjectMeta {
		Name:        "target-object",
		Namespace:   "target-namespace",
		Labels:      map[string]string {"wrong": "labels"},
		Annotations: map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
		},
	}
	source := &metav1.ObjectMeta {
		Name:        "source-object",
		Namespace:   "source-namespace",
		Annotations: map[string]string {
			ReplicationSourceAnnotation: "data-namespace/data-object",
		},
	}
	needed, err := rep.needsFromAnnotationsUpdate(target, source)
	assert.True(t, needed, "labels")
	assert.NoError(t, err, "labels")
}

func Test_needsAllowedAnnotationsUpdate(t *testing.T) {
	examples := [] struct {
		// the name of the test
		name   string
		// if update is needed
		needed bool
		// if error is expected
		err    bool
		// the source annotations
		source map[string]string
		// the target annotations
		target map[string]string
	}{{
		"no annotation",
		false,
		false,
		map[string]string {},
		map[string]string {},
	},{
		"same allow annotation",
		false,
		false,
		map[string]string {ReplicationAllowedAnnotation: "true"},
		map[string]string {ReplicationAllowedAnnotation: "true"},
	},{
		"missing allow annotation",
		true,
		false,
		map[string]string {ReplicationAllowedAnnotation: "true"},
		map[string]string {},
	},{
		"different allow annotation",
		true,
		false,
		map[string]string {ReplicationAllowedAnnotation: "false"},
		map[string]string {ReplicationAllowedAnnotation: "true"},
	},{
		"illformed allow annotation",
		false,
		true,
		map[string]string {ReplicationAllowedAnnotation: "other"},
		map[string]string {},
	},{
		"same allow namespaces annotation",
		false,
		false,
		map[string]string {AllowedNamespacesAnnotation: "same"},
		map[string]string {AllowedNamespacesAnnotation: "same"},
	},{
		"missing allow namespaces annotation",
		true,
		false,
		map[string]string {AllowedNamespacesAnnotation: "same"},
		map[string]string {},
	},{
		"different allow namespaces annotation",
		true,
		false,
		map[string]string {AllowedNamespacesAnnotation: "other"},
		map[string]string {AllowedNamespacesAnnotation: "same"},
	},{
		"illformed allow namespaces annotation",
		false,
		true,
		map[string]string {AllowedNamespacesAnnotation: "[other"},
		map[string]string {},
	}}
	rep := &replicatorProps {
		Name:     "object",
	}
	for _, example := range examples {
		target := &metav1.ObjectMeta {
			Name:        "target-object",
			Namespace:   "target-namespace",
			Labels:      getCopyLabels(),
			Annotations: example.target,
		}
		source := &metav1.ObjectMeta {
			Name:        "source-object",
			Namespace:   "source-namespace",
			Annotations: example.source,
		}
		needed, err := rep.needsAllowedAnnotationsUpdate(target, source)
		if example.needed {
			assert.True(t, needed, example.name)
			assert.NoError(t, err, example.name)
		} else {
			assert.False(t, needed, example.name)
		}
		if example.err {
			assert.Error(t, err, example.name)
		} else {
			assert.NoError(t, err, example.name)
		}
	}

	target := &metav1.ObjectMeta {
		Name:        "target-object",
		Namespace:   "target-namespace",
		Labels:      map[string]string {"wrong": "labels"},
		Annotations: map[string]string {ReplicationAllowedAnnotation: "true"},
	}
	source := &metav1.ObjectMeta {
		Name:        "source-object",
		Namespace:   "source-namespace",
		Annotations: map[string]string {ReplicationAllowedAnnotation: "true"},
	}
	needed, err := rep.needsAllowedAnnotationsUpdate(target, source)
	assert.True(t, needed, "labels")
	assert.NoError(t, err, "labels")
}

func Test_isReplicatedBy(t *testing.T) {
	examples := [] struct {
		// the name of the test
		name        string
		// if is replicated by
		replicated  bool
		// the target annotations
		annotations map[string]string
	}{{
		"not replicated",
		false,
		map[string]string{},
	},{
		"replicated",
		true,
		map[string]string{CreatedByAnnotation: "source-namespace/source-object"},
	},{
		"replicated by other",
		false,
		map[string]string{CreatedByAnnotation: "other-namespace/other-object"},
	}}
	rep := &replicatorProps {
		Name:     "object",
	}
	for _, example := range examples {
		target := &metav1.ObjectMeta {
			Name:        "target-object",
			Namespace:   "target-namespace",
			Annotations: example.annotations,
		}
		source := &metav1.ObjectMeta {
			Name:        "source-object",
			Namespace:   "source-namespace",
		}
		replicated, err := rep.isReplicatedBy(target, source)
		if example.replicated {
			assert.True(t, replicated, example.name)
			assert.NoError(t, err, example.name)
		} else {
			assert.False(t, replicated, example.name)
			assert.Error(t, err, example.name)
		}
	}
}

func Test_isReplicatedTo(t *testing.T) {
	examples := [] struct {
		// the name of the test
		testName    string
		// if is replicated to
		replicated  bool
		// if an error is exptected
		err         bool
		// the name of the target
		name        string
		// the namespace of the target
		namespace   string
		// the source annotations
		annotations map[string]string
	}{{
		"not replicated",
		false,
		false,
		"target-object",
		"target-namespace",
		map[string]string{},
	},{
		"replicated",
		true,
		false,
		"target-object",
		"target-namespace",
		map[string]string{
			ReplicationTargetsAnnotation: "target-namespace/target-object",
		},
	},{
		"replicated list",
		true,
		false,
		"target-object",
		"target-namespace",
		map[string]string{
			ReplicationTargetsAnnotation: "first-namespace/first-object,target-namespace/target-object,last-namespace/last-object",
		},
	},{
		"not replicated (name)",
		false,
		false,
		"target-object",
		"target-namespace",
		map[string]string{
			ReplicationTargetsAnnotation: "target-namespace/other-object",
		},
	},{
		"not replicated (namespace)",
		false,
		false,
		"target-object",
		"target-namespace",
		map[string]string{
			ReplicationTargetsAnnotation: "other-namespace/target-object",
		},
	},{
		"replicated name",
		true,
		false,
		"target-object",
		"source-namespace",
		map[string]string{
			ReplicationTargetsAnnotation: "target-object",
		},
	},{
		"replicated name list",
		true,
		false,
		"target-object",
		"source-namespace",
		map[string]string{
			ReplicationTargetsAnnotation: "first-object,target-object,last-object",
		},
	},{
		"not replicated name (namespace)",
		false,
		false,
		"target-object",
		"target-namespace",
		map[string]string{
			ReplicationTargetsAnnotation: "target-object",
		},
	},{
		"not replicated name (name)",
		false,
		false,
		"target-object",
		"source-namespace",
		map[string]string{
			ReplicationTargetsAnnotation: "other-object",
		},
	},{
		"replicated namespace",
		true,
		false,
		"source-object",
		"target-namespace",
		map[string]string{
			TargetNamespacesAnnotation: "target-namespace",
		},
	},{
		"replicated namespace list",
		true,
		false,
		"source-object",
		"target-namespace",
		map[string]string{
			TargetNamespacesAnnotation: "first-namespace,target-namespace,last-namespace",
		},
	},{
		"not replicated namespace (namespace)",
		false,
		false,
		"source-object",
		"target-namespace",
		map[string]string{
			TargetNamespacesAnnotation: "other-namespace",
		},
	},{
		"not replicated namespace (name)",
		false,
		false,
		"target-object",
		"target-namespace",
		map[string]string{
			TargetNamespacesAnnotation: "target-namespace",
		},
	},{
		"replicated namespace pattern",
		true,
		false,
		"source-object",
		"target-namespace",
		map[string]string{
			TargetNamespacesAnnotation: "target-.*",
		},
	},{
		"replicated namespace pattern list",
		true,
		false,
		"source-object",
		"target-namespace",
		map[string]string{
			TargetNamespacesAnnotation: "first-.*,target-.*,last-.*",
		},
	},{
		"not replicated namespace pattern (namespace)",
		false,
		false,
		"source-object",
		"target-namespace",
		map[string]string{
			TargetNamespacesAnnotation: "other-.*",
		},
	},{
		"not replicated namespace pattern (name)",
		false,
		false,
		"target-object",
		"target-namespace",
		map[string]string{
			TargetNamespacesAnnotation: "target-.*",
		},
	},{
		"illformed target",
		false,
		true,
		"target-object",
		"target-namespace",
		map[string]string{
			ReplicationTargetsAnnotation: "target-namespace/target-object,target illformed",
		},
	},{
		"illformed pattern",
		false,
		true,
		"source-object",
		"target-namespace",
		map[string]string{
			TargetNamespacesAnnotation: "target-namespace,[target",
		},
	}}
	rep := &replicatorProps {
		Name:     "object",
	}
	for _, example := range examples {
		source := &metav1.ObjectMeta {
			Name:        "source-object",
			Namespace:   "source-namespace",
			Annotations: example.annotations,
		}
		target := &metav1.ObjectMeta {
			Name:        example.name,
			Namespace:   example.namespace,
		}
		replicated, err := rep.isReplicatedTo(source, target)
		if example.replicated {
			assert.True(t, replicated, example.testName)
			assert.NoError(t, err, example.testName)
		} else {
			assert.False(t, replicated, example.testName)
		}
		if example.err {
			assert.Error(t, err, example.name)
		} else {
			assert.NoError(t, err, example.name)
		}
	}
}

func Test_getReplicationTargets(t *testing.T) {
	examples := [] struct {
		// the name of the test
		name         string
		// if an error is exptec
		err          bool
		// the replicate-to annotation
		to           string
		// the replicate-to-namespaces annotation
		toNamespaces string
		// the expected targets
		targets      []string
		// matching tests for the target patters
		match        map[string]bool
		// namespace to pass the target patterns on
		namespaces   []string
		// expected targets from the target patterns
		matchTargets []string
	}{{
		name: "error to",
		err:  true,
		to:   "namespace/name/other",
	},{
		name:         "error to namespaces",
		err:          true,
		toNamespaces: "namespace/other",
	},{
		name:         "error to namespaces compilation",
		err:          true,
		toNamespaces: "[other",
	},{
		name: "to same",
		to:   "source-namespace/source-object",
	},{
		name: "to same name",
		to:   "source-object",
	},{
		name:         "to same namespace",
		toNamespaces: "source-namespace",
	},{
		name: "to repeated",
		to:   "target-namespace/target-object,target-namespace/target-object",
		targets: []string {
			"target-namespace/target-object",
		},
	},{
		name: "to name repeated",
		to:   "target-object,source-namespace/target-object,target-object",
		targets: []string {
			"source-namespace/target-object",
		},
	},{
		name:         "to namespace repeated",
		toNamespaces: "target-namespace,target-namespace",
		targets:      []string {
			"target-namespace/source-object",
		},
	},{
		name:    "to list",
		to:      "(first|second)-.*/target-object,namespace-[0-9]+/other-object",
		match:        map[string]bool {
			"source-namespace/source-object": false,
			"source-namespace/target-object": false,
			"first-namespace/target-object": true,
			"second-namespace/target-object": true,
			"first-namespace/source-object": false,
			"namespace-123/source-object": false,
			"namespace-123/other-object": true,
			"namespace-123a/other-object": false,
			"-namespace-123/other-object": false,
		},
	},{
		name:    "to pattern list",
		to:      "first-namespace/first-object,other-object,source-namespace/last-object",
		targets: []string {
			"first-namespace/first-object",
			"source-namespace/other-object",
			"source-namespace/last-object",
		},
	},{
		name:         "to namespaces list",
		toNamespaces: "first-namespace,second-namespace",
		targets:      []string {
			"first-namespace/source-object",
			"second-namespace/source-object",
		},
	},{
		name:         "to namespaces pattern list",
		toNamespaces: "(first|second)-.*,namespace-[0-9]+",
		match:        map[string]bool {
			"source-namespace/source-object": false,
			"first-namespace/source-object": true,
			"second-namespace/source-object": true,
			"third-namespace/source-object": false,
			"first-namespace/other-object": false,
			"namespace-123/source-object": true,
			"namespace-123/other-object": false,
			"namespace-abc/source-object": false,
			"namespace-123d/source-object": false,
			"-namespace-123/source-object": false,
		},
		namespaces:   []string {
			"source-namespace",
			"first-namespace",
			"second-namespace",
			"third-namespace",
			"namespace-123",
			"namespace-abc",
			"namespace-123d",
			"-namespace-123",
		},
		matchTargets: []string {
			"first-namespace/source-object",
			"second-namespace/source-object",
			"namespace-123/source-object",
		},
	},{
		name:         "combined",
		to:           "first-object,other-namespace/other-object,second-object,.*-namespace/last-object",
		toNamespaces: "(first|second)-.*,target-namespace,namespace-[0-9]+",
		targets:      []string {
			"other-namespace/other-object",
			"target-namespace/first-object",
			"target-namespace/second-object",
		},
		match:        map[string]bool {
			"first-namespace/other-object": false,
			"first-namespace/second-object": true,
			"other-namespace/second-object": false,
			"other-namespace/last-object": true,
			"namespace-123/first-object": true,
			"namespace-123/last-object": false,
		},
		namespaces:   []string {
			"source-namespace",
			"second-namespace",
			"other-namespace",
			"namespace-123",
			"namespace-abc",
		},
		matchTargets: []string {
			"second-namespace/first-object",
			"namespace-123/first-object",
			"second-namespace/second-object",
			"namespace-123/second-object",
			"source-namespace/last-object",
			"second-namespace/last-object",
			"other-namespace/last-object",
		},
	}}
	rep := &replicatorProps {
		Name:     "object",
	}
	for _, example := range examples {
		source := &metav1.ObjectMeta {
			Name:        "source-object",
			Namespace:   "source-namespace",
			Annotations: map[string]string{},
		}
		if example.to != "" {
			source.Annotations[ReplicationTargetsAnnotation] = example.to
		}
		if example.toNamespaces != "" {
			source.Annotations[TargetNamespacesAnnotation] = example.toNamespaces
		}
		targets, patterns, err := rep.getReplicationTargets(source)
		if example.err {
			assert.Error(t, err, example.name)
		}
		if example.targets == nil {
			assert.Empty(t, targets, example.name)
		} else {
			sort.Strings(example.targets)
			sort.Strings(targets)
			assert.Equal(t, example.targets, targets, example.name)
		}

		match := map[string]bool{}
		for value, _ := range example.match {
			m := false
			s := strings.Split(value, "/")
			target := &metav1.ObjectMeta {
				Name:      s[1],
				Namespace: s[0],
			}
			for _, pattern := range patterns {
				m1 := pattern.MatchString(value)
				m2 := pattern.Match(target)
				assert.Equal(t, m1, m2, example.name, value)
				if m1 {
					m = true
				}
			}
			match[value] = m
		}
		if example.match == nil {
			assert.Empty(t, match, example.name)
		} else {
			assert.Equal(t, example.match, match, example.name)
		}

		matchTargets := []string{}
		seen := map[string]bool{}
		for _, pattern := range patterns {
			e := map[string]bool{}
			for _, n := range example.namespaces {
				if v := pattern.MatchNamespace(n); v != "" {
					e[v] = true
				}
			}
			for _, v := range pattern.Targets(example.namespaces) {
				assert.True(t, e[v], example.name, v)
				delete(e, v)
				if !seen[v] {
					seen[v] = true
					matchTargets = append(matchTargets, v)
				}
			}
			assert.Empty(t, e, example.name, pattern.namespace.String(), pattern.name)
		}
		if example.matchTargets == nil {
			assert.Empty(t, matchTargets, example.name)
		} else {
			sort.Strings(example.matchTargets)
			sort.Strings(matchTargets)
			assert.Equal(t, example.matchTargets, matchTargets, example.name)
		}
	}
}

func Test_resolveAnnotation(t *testing.T) {
	examples := [] struct {
		// the name of the test
		name     string
		// the value of the annotations
		value    string
		// the expected result ("" if an error is expected)
		expected string
	}{{
		name: "absent",
	},{
		"name",
		"target-object",
		"source-namespace/target-object",
	},{
		"namespace and name",
		"target-namespace/target-object",
		"target-namespace/target-object",
	}}
	for _, example := range examples {
		source := &metav1.ObjectMeta {
			Name:        "source-object",
			Namespace:   "source-namespace",
			Annotations: map[string]string {},
		}
		if example.value != "" {
			source.Annotations["annotation"] = example.value
		}
		value, ok := resolveAnnotation(source, "annotation")
		if example.expected == "" {
			assert.False(t, ok, example.name)
		} else {
			assert.True(t, ok, example.name)
			assert.Equal(t, example.expected, value)
		}
	}
}

func Test_annotationRefersTo(t *testing.T) {
	examples := [] struct {
		// the name of the test
		testName  string
		// if the annotations refers to
		refers    bool
		// the value of the annotation
		value     string
		// the name of the reference tested
		name      string
		// the namespace of the reference tested
		namespace string
	}{{
		"absent",
		false,
		"",
		"target-object",
		"target-namespace",
	},{
		"refers name",
		true,
		"target-object",
		"target-object",
		"source-namespace",
	},{
		"not refers name (name)",
		false,
		"target-object",
		"other-object",
		"source-namespace",
	},{
		"not refers name (namespace)",
		false,
		"target-object",
		"target-object",
		"target-namespace",
	},{
		"refers namespace",
		true,
		"target-namespace/target-object",
		"target-object",
		"target-namespace",
	},{
		"not refers namespace (name)",
		false,
		"target-namespace/target-object",
		"other-object",
		"target-namespace",
	},{
		"not refers namespace (namespace)",
		false,
		"target-namespace/target-object",
		"target-object",
		"other-namespace",
	}}
	for _, example := range examples {
		source := &metav1.ObjectMeta {
			Name:        "source-object",
			Namespace:   "source-namespace",
			Annotations: map[string]string {},
		}
		if example.value != "" {
			source.Annotations["annotation"] = example.value
		}
		target := &metav1.ObjectMeta {
			Name:        example.name,
			Namespace:   example.namespace,
		}
		ok := annotationRefersTo(source, "annotation", target)
		if example.refers {
			assert.True(t, ok, example.testName)
		} else {
			assert.False(t, ok, example.testName)
		}
	}
}

func Test_updateDeprecatedAnnotations(t *testing.T) {
	previous := AnnotationsPrefix
	deprecated["test-deprecated"] = "test-replacement"
	defer func() {
		delete(deprecated, "test-deprecated")
		PrefixAnnotations(previous)
	}()
	examples := []struct{
		name   string
		prefix string
		update bool
		error  bool
		before map[string]string
		after  map[string]string
	}{{
		"nil",
		"nil/",
		false,
		false,
		nil,
		nil,
	}, {
		"empty",
		"empty/",
		false,
		false,
		map[string]string{},
		map[string]string{},
	}, {
		"ok",
		"ok/",
		false,
		false,
		map[string]string{
			"ok/replicate-from": "test-from",
			"ok/replicate-to": "test-to",
			"other-annotation": "other-value",
		},
		map[string]string{
			"ok/replicate-from": "test-from",
			"ok/replicate-to": "test-to",
			"other-annotation": "other-value",
		},
	}, {
		"deprecated",
		"deprecated/",
		true,
		false,
		map[string]string{
			"deprecated/replicate-from": "test-from",
			"deprecated/test-deprecated": "test-value",
			"other-annotation": "other-value",

		},
		map[string]string{
			"deprecated/replicate-from": "test-from",
			"deprecated/test-replacement": "test-value",
			"other-annotation": "other-value",

		},
	}, {
		"invalid",
		"invalid/",
		false,
		true,
		map[string]string{
			"invalid/replicate-from": "test-from",
			"invalid/test-invalid": "test-value",
			"other-annotation": "other-value",

		},
		map[string]string{
			"invalid/replicate-from": "test-from",
			"invalid/test-invalid": "test-value",
			"other-annotation": "other-value",

		},
	},{
		"empty no slash",
		"empty-",
		false,
		false,
		map[string]string{},
		map[string]string{},
	}, {
		"ok no slash",
		"ok-",
		false,
		false,
		map[string]string{
			"ok-replicate-from": "test-from",
			"ok-replicate-to": "test-to",
			"other-annotation": "other-value",
		},
		map[string]string{
			"ok-replicate-from": "test-from",
			"ok-replicate-to": "test-to",
			"other-annotation": "other-value",
		},
	}, {
		"deprecated no slash",
		"deprecated-",
		true,
		false,
		map[string]string{
			"deprecated-replicate-from": "test-from",
			"deprecated-test-deprecated": "test-value",
			"other-annotation": "other-value",

		},
		map[string]string{
			"deprecated-replicate-from": "test-from",
			"deprecated-test-replacement": "test-value",
			"other-annotation": "other-value",

		},
	}, {
		"invalid no slash",
		"invalid-",
		false,
		false,
		map[string]string{
			"invalid-replicate-from": "test-from",
			"invalid-test-invalid": "test-value",
			"other-annotation": "other-value",

		},
		map[string]string{
			"invalid-replicate-from": "test-from",
			"invalid-test-invalid": "test-value",
			"other-annotation": "other-value",

		},
	}}
	for _, example := range examples {
		PrefixAnnotations(example.prefix)
		meta := &metav1.ObjectMeta {
			Namespace:   "test-namespace",
			Name:        "test-name",
			Annotations: example.before,
		}
		update, err := updateDeprecatedAnnotations(meta)
		if example.error {
			assert.False(t, example.update, example.name)
			example.after[CheckedAnnotation] = "error"
			assert.Error(t, err, example.name)
			assert.False(t, update, example.name)
			assert.Equal(t, example.after, meta.Annotations, example.name)
			update, err = updateDeprecatedAnnotations(meta)
			assert.Error(t, err, example.name)
			assert.False(t, update, example.name)
			assert.Equal(t, example.after, meta.Annotations, example.name)
		} else if example.update {
			assert.False(t, example.error, example.name)
			example.after[CheckedAnnotation] = "update"
			assert.NoError(t, err, example.name)
			assert.True(t, update, example.name)
			assert.Equal(t, example.after, meta.Annotations, example.name)
			update, err = updateDeprecatedAnnotations(meta)
			assert.NoError(t, err, example.name)
			assert.True(t, update, example.name)
			assert.Equal(t, example.after, meta.Annotations, example.name)
		} else {
			assert.False(t, example.error, example.name)
			if example.after != nil {
				example.after[CheckedAnnotation] = "valid"
			}
			assert.NoError(t, err, example.name)
			assert.False(t, update, example.name)
			assert.Equal(t, example.after, meta.Annotations, example.name)
			update, err = updateDeprecatedAnnotations(meta)
			assert.NoError(t, err, example.name)
			assert.False(t, update, example.name)
			assert.Equal(t, example.after, meta.Annotations, example.name)
		}
	}
}
