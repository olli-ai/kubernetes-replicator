package replicate

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	if os.Getenv("LOG") != "true" {
		log.SetOutput(ioutil.Discard)
	}
	os.Exit(m.Run())
}

var validTimesptamp = regexp.MustCompile(`^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d`)
func IsTimestamp(t *testing.T, value string, msgAndArgs ...interface{}) bool {
	if !validTimesptamp.MatchString(value) {
		assert.Fail(t, "should be a timestamp", msgAndArgs...)
		return false
	} else {
		return true
	}
}

// test replicate-from, replication-allowed and replication-allowed-namespaces annotations
func TestFromAnnotation(t *testing.T) {
	examples := []struct{
		// the name of the test
		name        string
		// if the object should be replicated
		replicated  bool
		// gloabll --allow-all option
		allowAll    bool
		// source annotations
		annotations map[string]string
		// target namespace (default to target-namespace)
		namespace   string
		// target replicate-from annotation (default to source-namespace/source-name)
		from        string
	}{{
		name:       "no annotations",
		replicated: false,
	}, {
		name:       "allow all",
		replicated: true,
		allowAll:   true,
	}, {
		name:       "allow all but other annotation",
		replicated: false,
		allowAll:   true,
		annotations: map[string]string {
			AnnotationsPrefix + "other-annotations": "true",
		},
	}, {
		name:       "allow",
		replicated: true,
		annotations: map[string]string {
			ReplicationAllowedAnnotation: "true",
		},
	}, {
		name:       "allow but other annotation",
		replicated: false,
		annotations: map[string]string {
			ReplicationAllowedAnnotation: "true",
			AnnotationsPrefix + "other-annotations": "true",
		},
	}, {
		name:       "allow same namespace",
		replicated: true,
		annotations: map[string]string {
			ReplicationAllowedAnnotation: "true",
		},
		namespace:  "source-namespace",
		from:       "source-name",
	}, {
		name:       "disallow",
		replicated: false,
		annotations: map[string]string {
			ReplicationAllowedAnnotation: "false",
		},
	}, {
		name:       "allow all but disallow",
		replicated: false,
		allowAll:   true,
		annotations: map[string]string {
			ReplicationAllowedAnnotation: "false",
		},
	}, {
		name:       "allow wrong format",
		replicated: false,
		annotations: map[string]string {
			ReplicationAllowedAnnotation: "other",
		},
	}, {
		name:       "allow all but allow wrong format",
		replicated: false,
		allowAll:   true,
		annotations: map[string]string {
			ReplicationAllowedAnnotation: "other",
		},
	}, {
		name:       "allow namespace",
		replicated: true,
		annotations: map[string]string {
			AllowedNamespacesAnnotation: "target-namespace",
		},
	}, {
		name:       "allow other namespace",
		replicated: false,
		annotations: map[string]string {
			AllowedNamespacesAnnotation: "other-namespace",
		},
	}, {
		name:       "allow all but allow other namespace",
		replicated: false,
		allowAll:   true,
		annotations: map[string]string {
			AllowedNamespacesAnnotation: "other-namespace",
		},
	}, {
		name:       "allow namespace list",
		replicated: true,
		annotations: map[string]string {
			AllowedNamespacesAnnotation: "first-namespace,target-namespace,last-namespace",
		},
	}, {
		name:       "allow namespace pattern",
		replicated: true,
		annotations: map[string]string {
			AllowedNamespacesAnnotation: "target-.*",
		},
	}, {
		name:       "allow other pattern",
		replicated: false,
		annotations: map[string]string {
			AllowedNamespacesAnnotation: "other-.*",
		},
	}, {
		name:       "allow all but allow other pattern",
		replicated: false,
		allowAll:   true,
		annotations: map[string]string {
			AllowedNamespacesAnnotation: "other-.*",
		},
	}, {
		name:       "allow namespace pattern list",
		replicated: true,
		annotations: map[string]string {
			AllowedNamespacesAnnotation: "first-.*,target-.*,last-.*",
		},
	}}
	for _, example := range examples {
		if example.namespace == "" {
			example.namespace = "target-namespace"
		}
		if example.from == "" {
			example.from = "source-namespace/source-name"
		}
		// create source object
		source := func (repl *FakeReplicator) bool {
			err := repl.SetAddFake(NewFake(
				"source-namespace",
				"source-name",
				"source-data",
				example.annotations,
			))
			return assert.NoError(t, err, example.name)
		}
		// create target object
		target := func (repl *FakeReplicator) bool {
			err := repl.SetAddFake(NewFake(
				example.namespace,
				"target-name",
				"target-data",
				map[string]string {
					ReplicationSourceAnnotation: example.from,
				},
			))
			return assert.NoError(t, err, example.name)
		}
		// test that everything went fine
		test := func (repl *FakeReplicator) bool {
			// source and target exist
			source, err := repl.GetFake("source-namespace", "source-name")
			if !assert.NoError(t, err, example.name) || !assert.NotNil(t, source, example.name) {
				return false
			}
			target, err := repl.GetFake(example.namespace, "target-name")
			if !assert.NoError(t, err, example.name) || !assert.NotNil(t, target, example.name) {
				return false
			}
			// target has the right data and annotations
			atV, atOk := target.Annotations[ReplicationTimeAnnotation]
			vV, vOk := target.Annotations[ReplicatedVersionAnnotation]
			if example.replicated {
				assert.Equal(t, "source-data", target.Data, example.name)
				if assert.True(t, atOk, example.name) {
					IsTimestamp(t, atV, example.name)
				}
				if assert.True(t, vOk, example.name) {
					assert.Equal(t, source.ResourceVersion, vV, example.name)
				}
			} else {
				assert.Equal(t, "target-data", target.Data, example.name)
				assert.False(t, atOk, example.name)
				assert.False(t, vOk, example.name)
			}
			// test that redondant calls have no impact
			calls := repl.Calls()
			if !assert.NoError(t, repl.AddFake(source), example.name) {
				return false
			}
			if !assert.NoError(t, repl.AddFake(target), example.name) {
				return false
			}
			assert.Equal(t, calls, repl.Calls(), example.name)
			return true
		}
		// delete the source and test what happens
		clear := func (repl *FakeReplicator) bool {
			source, err := repl.GetFake("source-namespace", "source-name")
			if !assert.NoError(t, err, example.name) || !assert.NotNil(t, source, example.name) {
				return false
			}
			if !assert.NoError(t, repl.UnsetDeleteFake(source), example.name) {
				return false
			}
			target, err := repl.GetFake(example.namespace, "target-name")
			if !assert.NoError(t, err, example.name) || !assert.NotNil(t, target, example.name) {
				return false
			}
			// the target has lost its data and its annotations
			atV, atOk := target.Annotations[ReplicationTimeAnnotation]
			_, vOk := target.Annotations[ReplicatedVersionAnnotation]
			if example.replicated {
				assert.Equal(t, "", target.Data, example.name)
				if assert.True(t, atOk, example.name) {
					IsTimestamp(t, atV, example.name)
				}
			} else {
				assert.Equal(t, "target-data", target.Data, example.name)
				assert.False(t, atOk, example.name)
			}
			assert.False(t, vOk, example.name)
			// test that redondant calls have no impact
			calls := repl.Calls()
			if !assert.NoError(t, repl.DeleteFake(source), example.name) {
				return false
			}
			if !assert.NoError(t, repl.AddFake(target), example.name) {
				return false
			}
			assert.Equal(t, calls, repl.Calls(), example.name)
			return true
		}
		// try with different orders
		repl := NewFakeReplicator(t, example.allowAll)
		assert.True(t,
			source(repl) &&
			target(repl) &&
			test(repl) &&
			target(repl) &&
			test(repl) &&
			clear(repl) &&
			source(repl) &&
			test(repl),
			example.name)
		repl = NewFakeReplicator(t, example.allowAll)
		assert.True(t,
			target(repl) &&
			source(repl) &&
			test(repl) &&
			target(repl) &&
			test(repl) &&
			clear(repl) &&
			source(repl) &&
			test(repl),
			example.name)
	}
}

// test replicate-to and replication-to-namespaces annotations
func TestToAnnotation(t *testing.T) {
	examples := []struct{
		// the name of the test
		testName    string
		// the name of the target, "" if none expected
		name        string
		// the annotations of the source
		annotations map[string]string
		// the target namespace, source-namespace by default
		namespace   string
	}{{
		testName:    "no annotation",
		namespace:   "other-namespace",
	},{
		testName:    "no namespace (to annotation)",
		annotations: map[string]string {
			ReplicationTargetsAnnotation: "target-namespace/target-name",
		},
	},{
		testName:    "no namespace (to namespace annotation)",
		annotations: map[string]string {
			TargetNamespacesAnnotation: "target-namespace",
		},
	},{
		testName:    "same namespace",
		name:        "target-name",
		annotations: map[string]string {
			ReplicationTargetsAnnotation: "target-name",
		},
	},{
		testName:    "same name",
		name:        "source-name",
		annotations: map[string]string {
			TargetNamespacesAnnotation: "target-namespace",
		},
		namespace:   "target-namespace",
	},{
		testName:    "to annotation",
		name:        "target-name",
		annotations: map[string]string {
			ReplicationTargetsAnnotation: "target-namespace/target-name",
		},
		namespace:   "target-namespace",
	},{
		testName:    "both annotations",
		name:        "target-name",
		annotations: map[string]string {
			ReplicationTargetsAnnotation: "target-name",
			TargetNamespacesAnnotation: "target-namespace",
		},
		namespace:   "target-namespace",
	},{
		testName:    "both annotations but other annotation",
		annotations: map[string]string {
			ReplicationTargetsAnnotation: "target-name",
			TargetNamespacesAnnotation: "target-namespace",
			AnnotationsPrefix + "other-annotations": "true",
		},
		namespace:   "target-namespace",
	},{
		testName:    "pattern to annotations",
		name:        "target-name",
		annotations: map[string]string {
			ReplicationTargetsAnnotation: "target-.*/target-name",
		},
		namespace:   "target-namespace",
	},{
		testName:    "pattern to namespace annotations",
		name:        "source-name",
		annotations: map[string]string {
			TargetNamespacesAnnotation: "target-.*",
		},
		namespace:   "target-namespace",
	},{
		testName:    "pattern both annotations",
		name:        "target-name",
		annotations: map[string]string {
			ReplicationTargetsAnnotation: "target-name",
			TargetNamespacesAnnotation: "target-.*",
		},
		namespace:   "target-namespace",
	},{
		testName:    "list to annotation",
		name:        "target-name",
		annotations: map[string]string {
			ReplicationTargetsAnnotation: "first-namespace/first-name,target-namespace/target-name,last-namespace/last-name",
		},
		namespace:   "target-namespace",
	},{
		testName:    "list to namespace annotation",
		name:        "source-name",
		annotations: map[string]string {
			TargetNamespacesAnnotation: "first-namespace,target-namespace,last-namespace",
		},
		namespace:   "target-namespace",
	}}
	for _, example := range examples {
		if example.namespace == "" {
			example.namespace = "source-namespace"
		}
		// create the souce object
		source := func (repl *FakeReplicator) bool {
			// create the source namespace
			// just for the sake of testing it does not has any effect
			if example.namespace != "source-namespace" {
				err := repl.AddNamespace("source-namespace")
				if !assert.NoError(t, err, example.testName) {
					return false
				}
			}
			err := repl.SetAddFake(NewFake(
				"source-namespace",
				"source-name",
				"source-data",
				example.annotations,
			))
			return assert.NoError(t, err, example.testName)
		}
		// create the target namespace
		target := func (repl *FakeReplicator) bool {
			err := repl.AddNamespace(example.namespace)
			return assert.NoError(t, err, example.testName)
		}
		// test that the state is the one expected
		test := func (repl *FakeReplicator) bool {
			source, err := repl.GetFake("source-namespace", "source-name")
			if !assert.NoError(t, err, example.testName) || !assert.NotNil(t, source, example.testName) {
				return false
			}
			var target *FakeObject
			expected := map[string]bool{"source-namespace/source-name": true}
			if example.name != "" {
				target, err = repl.GetFake(example.namespace, example.name)
				if !assert.NoError(t, err, example.testName) || !assert.NotNil(t, target, example.testName) {
					return false
				}
				expected[target.Key()] = true
				// test that target has the right data and annotations
				atV, atOk := target.Annotations[ReplicationTimeAnnotation]
				byV, byOk := target.Annotations[CreatedByAnnotation]
				vV, vOk := target.Annotations[ReplicatedVersionAnnotation]
				assert.Equal(t, "source-data", target.Data, example.testName)
				if assert.True(t, atOk, example.testName) {
					IsTimestamp(t, atV, example.testName)
				}
				if assert.True(t, byOk, example.testName) {
					assert.Equal(t, source.Key(), byV, example.testName)
				}
				if assert.True(t, vOk, example.testName) {
					assert.Equal(t, source.ResourceVersion, vV, example.testName)
				}
				// delete the target and test again
				if !assert.NoError(t, repl.UnsetDeleteFake(target), example.testName) {
					return false
				}
				target, err = repl.GetFake(example.namespace, example.name)
				if !assert.NoError(t, err, example.testName) || !assert.NotNil(t, target, example.testName) {
					return false
				}
				atV, atOk = target.Annotations[ReplicationTimeAnnotation]
				byV, byOk = target.Annotations[CreatedByAnnotation]
				vV, vOk = target.Annotations[ReplicatedVersionAnnotation]
				assert.Equal(t, "source-data", target.Data, example.testName)
				if assert.True(t, atOk, example.testName) {
					IsTimestamp(t, atV, example.testName)
				}
				if assert.True(t, byOk, example.testName) {
					assert.Equal(t, source.Key(), byV, example.testName)
				}
				if assert.True(t, vOk, example.testName) {
					assert.Equal(t, source.ResourceVersion, vV, example.testName)
				}
			}
			// test that the existing objects are the expected ones
			found := map[string]bool{}
			for key, _ := range repl.Versions() {
				found[key] = true
			}
			assert.Equal(t, expected, found, example.testName)
			// test that redondant calls have no impact
			calls := repl.Calls()
			if !assert.NoError(t, repl.AddNamespace(example.namespace), example.testName) {
				return false
			}
			if !assert.NoError(t, repl.AddFake(source), example.testName) {
				return false
			}
			if example.name != "" && !assert.NoError(t, repl.AddFake(target), example.testName) {
				return false
			}
			assert.Equal(t, calls, repl.Calls(), example.name)
			return true
		}
		// clears the source and test what happens
		clear := func (repl *FakeReplicator) bool {
			source, err := repl.GetFake("source-namespace", "source-name")
			if !assert.NoError(t, err, example.testName) || !assert.NotNil(t, source, example.testName) {
				return false
			}
			var target *FakeObject
			if example.name != "" {
				target, err = repl.GetFake(example.namespace, example.name)
				if !assert.NoError(t, err, example.testName) || !assert.NotNil(t, target, example.testName) {
					return false
				}
			}
			if !assert.NoError(t, repl.UnsetDeleteFake(source), example.testName) {
				return false
			}
			// test that target does not exist anymore
			if example.name != "" {
				if target, err := repl.GetFake(example.namespace, example.name); !assert.NoError(t, err, example.testName) || !assert.Nil(t, target, example.testName) {
					return false
				}
			}
			// test that the existing objects are the expected ones
			found := map[string]bool{}
			for key, _ := range repl.Versions() {
				found[key] = true
			}
			assert.Equal(t, map[string]bool{}, found, example.testName)
			// test that redondant calls have no impact
			calls := repl.Calls()
			if !assert.NoError(t, repl.AddNamespace(example.namespace), example.testName) {
				return false
			}
			if !assert.NoError(t, repl.DeleteFake(source), example.testName) {
				return false
			}
			if target != nil && !assert.NoError(t, repl.DeleteFake(target), example.testName) {
				return false
			}
			assert.Equal(t, calls, repl.Calls(), example.testName)
			return true
		}
		// try in different orders
		repl := NewFakeReplicator(t, false)
		assert.True(t,
			source(repl) &&
			target(repl) &&
			test(repl) &&
			clear(repl) &&
			source(repl) &&
			test(repl),
			example.testName)
		repl = NewFakeReplicator(t, false)
		assert.True(t,
			target(repl) &&
			source(repl) &&
			test(repl) &&
			clear(repl) &&
			source(repl) &&
			test(repl),
			example.testName)
	}
}

// tests the combination of replicate-from and replicate-to annotations
func TestFromToAnnotation(t *testing.T) {
	examples := []struct{
		// name of the test
		name            string
		// if the target should be replicated with the source
		replicated      bool
		// --allow-all global option
		allowAll        bool
		// source annotations
		source          map[string]string
		// name and namespace of the middle object
		middleName      string
		middleNamespace string
		// middle annotations
		middle          map[string]string
		// name and namespace of the target object
		targetName      string
		targetNamespace string
	}{{
		name:            "from annotation, no allowed",
		replicated:      false,
		middle:          map[string]string{
			ReplicationSourceAnnotation: "source-namespace/source-name",
			ReplicationTargetsAnnotation: "target-namespace/target-name",
		},
		targetName:      "target-name",
	},{
		name:            "from annotation, --allow-all",
		replicated:      true,
		allowAll:        true,
		middle:          map[string]string{
			ReplicationSourceAnnotation: "source-namespace/source-name",
			ReplicationTargetsAnnotation: "target-namespace/target-name",
		},
		targetName:      "target-name",
	},{
		name:            "from annotation, allowed",
		replicated:      true,
		source:          map[string]string{
			ReplicationAllowedAnnotation: "true",
		},
		middle:          map[string]string{
			ReplicationSourceAnnotation: "source-namespace/source-name",
			ReplicationTargetsAnnotation: "target-namespace/target-name",
		},
		targetName:      "target-name",
	},{
		name:            "from annotation, allowed, but other annotation",
		replicated:      false,
		source:          map[string]string{
			ReplicationAllowedAnnotation: "true",
			AnnotationsPrefix + "other-annotations": "true",
		},
		middle:          map[string]string{
			ReplicationSourceAnnotation: "source-namespace/source-name",
			ReplicationTargetsAnnotation: "target-namespace/target-name",
		},
		targetName:      "target-name",
	},{
		name:            "from annotation, allowed namespace",
		replicated:      true,
		source:          map[string]string{
			AllowedNamespacesAnnotation: "target-namespace",
		},
		middle:          map[string]string{
			ReplicationSourceAnnotation: "source-namespace/source-name",
			ReplicationTargetsAnnotation: "target-namespace/target-name",
		},
		targetName:      "target-name",
	},{
		name:            "from annotation, allowed middle",
		replicated:      false,
		source:          map[string]string{
			AllowedNamespacesAnnotation: "niddle-namespace",
		},
		middle:          map[string]string{
			ReplicationSourceAnnotation: "source-namespace/source-name",
			ReplicationTargetsAnnotation: "target-namespace/target-name",
		},
		targetName:      "target-name",
	},{
		name:            "from annotation, same namespace",
		replicated:      true,
		allowAll:        true,
		middleNamespace: "source-namespace",
		middle:          map[string]string{
			ReplicationSourceAnnotation: "source-name",
			ReplicationTargetsAnnotation: "target-namespace/target-name",
		},
		targetName:      "target-name",
	},{
		name:            "to annotation, same namespace",
		replicated:      true,
		allowAll:        true,
		middle:          map[string]string{
			ReplicationSourceAnnotation: "source-namespace/source-name",
			ReplicationTargetsAnnotation: "target-name",
		},
		targetName:      "target-name",
		targetNamespace: "middle-namespace",
	},{
		name:            "to annotation, same name",
		replicated:      true,
		allowAll:        true,
		middle:          map[string]string{
			ReplicationSourceAnnotation: "source-namespace/source-name",
			TargetNamespacesAnnotation: "target-namespace",
		},
		targetName:      "middle-name",
		targetNamespace: "target-namespace",
	}}
	for _, example := range examples {
		if example.middleNamespace == "" {
			example.middleNamespace = "middle-namespace"
		}
		if example.middleName == "" {
			example.middleName = "middle-name"
		}
		if example.targetNamespace == "" {
			example.targetNamespace = "target-namespace"
		}
		// create source object, with the data
		source := func (repl *FakeReplicator) bool {
			err := repl.SetAddFake(NewFake(
				"source-namespace",
				"source-name",
				"source-data",
				example.source,
			))
			if !assert.NoError(t, err, example.name) {
				return false
			}
			return true
		}
		// create middle object, with the "replicate-to" annotation
		middle := func (repl *FakeReplicator) bool {
			key := fmt.Sprintf("%s/%s", example.targetNamespace, example.targetName)
			version := repl.Versions()[key]
			err := repl.SetAddFake(NewFake(
				example.middleNamespace,
				example.middleName,
				"middle-data",
				example.middle,
			))
			if !assert.NoError(t, err, example.name) {
				return false
			}
			// if the target is updated, needs to fprocess it again
			if repl.Versions()[key] != version {
				target, err := repl.GetFake(example.targetNamespace, example.targetName)
				if !assert.NoError(t, err, example.name) || !assert.NotNil(t, target, example.name) {
					return false
				}
				if !assert.NoError(t, repl.AddFake(target), example.name) {
					return false
				}
			}
			return true
		}
		// create the target namespace
		target := func (repl *FakeReplicator) bool {
			key := example.targetNamespace + "/" + example.targetName
			version := repl.Versions()[key]
			err := repl.AddNamespace(example.targetNamespace)
			if !assert.NoError(t, err, example.name) {
				return false
			}
			// if the target is updated, needs to fprocess it again
			if repl.Versions()[key] != version {
				target, err := repl.GetFake(example.targetNamespace, example.targetName)
				if !assert.NoError(t, err, example.name) || !assert.NotNil(t, target, example.name) {
					return false
				}
				if !assert.NoError(t, repl.AddFake(target), example.name) {
					return false
				}
			}
			return true
		}
		// checks if everything is fine
		test := func (repl *FakeReplicator) bool {
			source, err := repl.GetFake("source-namespace", "source-name")
			if !assert.NoError(t, err, example.name) {
				return false
			}
			middle, err := repl.GetFake(example.middleNamespace, example.middleName)
			if !assert.NoError(t, err, example.name) || !assert.NotNil(t, middle, example.name) {
				return false
			}
			assert.Equal(t, "middle-data", middle.Data, example.name)
			target, err := repl.GetFake(example.targetNamespace, example.targetName)
			if !assert.NoError(t, err, example.name) || !assert.NotNil(t, target, example.name) {
				return false
			}
			// check the annotations
			fromV, fromOk := target.Annotations[ReplicationSourceAnnotation]
			byV, byOk := target.Annotations[CreatedByAnnotation]
			atV, atOk := target.Annotations[ReplicationTimeAnnotation]
			vV, vOk := target.Annotations[ReplicatedVersionAnnotation]
			if assert.True(t, fromOk, example.name) {
				assert.Equal(t, "source-namespace/source-name", fromV, example.name)
			}
			if assert.True(t, byOk, example.name) {
				assert.Equal(t, middle.Key(), byV, example.name)
			}
			if source != nil && example.replicated {
				assert.Equal(t, "source-data", target.Data, example.name)
				if assert.True(t, atOk, example.name) {
					IsTimestamp(t, atV, example.name)
				}
				if assert.True(t, vOk, example.name) {
					assert.Equal(t, source.ResourceVersion, vV, example.name)
				}
			} else {
				assert.Equal(t, "", target.Data, example.name)
				assert.False(t, atOk, example.name)
				assert.False(t, vOk, example.name)
			}
			// test that redondant calls have no impact
			calls := repl.Calls()
			if !assert.NoError(t, repl.AddNamespace(example.targetNamespace), example.name) {
				return false
			}
			if source != nil && !assert.NoError(t, repl.AddFake(source), example.name) {
				return false
			}
			if !assert.NoError(t, repl.SetAddFake(middle.Update("middle-data", nil)), example.name) {
				return false
			}
			if !assert.NoError(t, repl.AddFake(target), example.name) {
				return false
			}
			assert.Equal(t, calls, repl.Calls(), example.name)
			return true
		}
		// remove the source object and check what happens
		clearSource := func (repl *FakeReplicator) bool {
			source, err := repl.GetFake("source-namespace", "source-name")
			if !assert.NoError(t, err, example.name) || !assert.NotNil(t, source, example.name) {
				return false
			}
			middle, err := repl.GetFake(example.middleNamespace, example.middleName)
			if !assert.NoError(t, err, example.name) || !assert.NotNil(t, middle, example.name) {
				return false
			}
			target, err := repl.GetFake(example.targetNamespace, example.targetName)
			if !assert.NoError(t, err, example.name) || !assert.NotNil(t, target, example.name) {
				return false
			}
			if !assert.NoError(t, repl.UnsetDeleteFake(source), example.name) {
				return false
			}
			// test that target lost its data
			if target, err = repl.GetFake(example.targetNamespace, example.targetName); !assert.NoError(t, err, example.name) || !assert.NotNil(t, target, example.name) {
				return false
			}
			fromV, fromOk := target.Annotations[ReplicationSourceAnnotation]
			byV, byOk := target.Annotations[CreatedByAnnotation]
			atV, atOk := target.Annotations[ReplicationTimeAnnotation]
			_, vOk := target.Annotations[ReplicatedVersionAnnotation]
			assert.Equal(t, "", target.Data, example.name)
			if assert.True(t, fromOk, example.name) {
				assert.Equal(t, "source-namespace/source-name", fromV, example.name)
			}
			if assert.True(t, byOk, example.name) {
				assert.Equal(t, middle.Key(), byV, example.name)
			}
			if example.replicated {
				if assert.True(t, atOk, example.name) {
					IsTimestamp(t, atV, example.name)
				}
			} else {
				assert.False(t, atOk, example.name)
			}
			assert.False(t, vOk, example.name)
			// test that redondant calls have no impact
			calls := repl.Calls()
			if !assert.NoError(t, repl.AddNamespace(example.targetNamespace), example.name) {
				return false
			}
			if !assert.NoError(t, repl.DeleteFake(source), example.name) {
				return false
			}
			if !assert.NoError(t, repl.AddFake(middle), example.name) {
				return false
			}
			if !assert.NoError(t, repl.AddFake(target), example.name) {
				return false
			}
			assert.Equal(t, calls, repl.Calls(), example.name)
			return true
		}
		// remove the middle object and check what happens
		clearMiddle := func (repl *FakeReplicator) bool {
			source, err := repl.GetFake("source-namespace", "source-name")
			if !assert.NoError(t, err, example.name) {
				return false
			}
			middle, err := repl.GetFake(example.middleNamespace, example.middleName)
			if !assert.NoError(t, err, example.name) || !assert.NotNil(t, middle, example.name) {
				return false
			}
			target, err := repl.GetFake(example.targetNamespace, example.targetName)
			if !assert.NoError(t, err, example.name) || !assert.NotNil(t, target, example.name) {
				return false
			}
			if !assert.NoError(t, repl.UnsetDeleteFake(middle), example.name) {
				return false
			}
			// test that target is deleted
			if target, err := repl.GetFake(example.targetNamespace, example.targetName); !assert.NoError(t, err, example.name) || !assert.Nil(t, target, example.name) {
				return false
			}
			// test that redondant calls have no impact
			calls := repl.Calls()
			if !assert.NoError(t, repl.AddNamespace(example.targetNamespace), example.name) {
				return false
			}
			if source != nil && !assert.NoError(t, repl.AddFake(source), example.name) {
				return false
			}
			if !assert.NoError(t, repl.DeleteFake(middle), example.name) {
				return false
			}
			if !assert.NoError(t, repl.DeleteFake(target), example.name) {
				return false
			}
			assert.Equal(t, calls, repl.Calls(), example.name)
			return true
		}
		// try in different orders
		repl := NewFakeReplicator(t, example.allowAll)
		assert.True(t,
			source(repl) &&
			middle(repl) &&
			target(repl) &&
			test(repl) &&
			clearSource(repl) &&
			source(repl) &&
			test(repl),
			example.name)
		repl = NewFakeReplicator(t, example.allowAll)
		assert.True(t,
			source(repl) &&
			target(repl) &&
			middle(repl) &&
			test(repl) &&
			clearMiddle(repl) &&
			middle(repl),
			example.name)
		repl = NewFakeReplicator(t, example.allowAll)
		assert.True(t,
			middle(repl) &&
			source(repl) &&
			target(repl) &&
			test(repl) &&
			clearSource(repl) &&
			source(repl) &&
			test(repl),
			example.name)
		repl = NewFakeReplicator(t, example.allowAll)
		assert.True(t,
			middle(repl) &&
			target(repl) &&
			test(repl) &&
			source(repl) &&
			test(repl) &&
			clearMiddle(repl) &&
			middle(repl) &&
			test(repl),
			example.name)
		repl = NewFakeReplicator(t, example.allowAll)
		assert.True(t,
			target(repl) &&
			source(repl) &&
			middle(repl) &&
			test(repl) &&
			clearSource(repl) &&
			source(repl) &&
			test(repl),
			example.name)
		repl = NewFakeReplicator(t, example.allowAll)
		assert.True(t,
			target(repl) &&
			middle(repl) &&
			test(repl) &&
			source(repl) &&
			test(repl) &&
			clearMiddle(repl) &&
			middle(repl) &&
			test(repl),
			example.name)
	}
}

// test replicate-to with many targets and data update
func TestToAnnotation_ManyTargets(t *testing.T) {
	beforeNs := []string {
		"source-namespace",
		"other-namespace",
		"pattern-ns1",
		"namespace-123",
		"namespace-abc",
	}
	beforeKeys := []string {
		"other-namespace/other-name",
		"pattern-ns1/pattern-name",
		"namespace-123/target-name1",
		"namespace-123/target-name2",
	}
	afterNs := []string {
		"target-namespace",
		"pattern-ns2",
		"namespace-456",
		"namespace-xyz",
	}
	afterKeys := []string {
		"target-namespace/target-name1",
		"target-namespace/target-name2",
		"pattern-ns2/pattern-name",
		"namespace-456/target-name1",
		"namespace-456/target-name2",
	}
	repl := NewFakeReplicator(t, false)

	var err error
	source := NewFake("source-namespace", "source-name", "before-data",
		map[string]string {
			ReplicationTargetsAnnotation: "pattern-.*/pattern-name,target-name1,target-name2,other-namespace/other-name",
			TargetNamespacesAnnotation: "target-namespace,namespace-[0-9]+",
		})
	calls := 0
	for _, ns := range beforeNs {
		require.NoError(t, repl.AddNamespace(ns))
	}
	assert.Equal(t, calls, repl.Calls())
	calls = repl.Calls() + len(beforeKeys)
	require.NoError(t, repl.SetAddFake(source))
	assert.Equal(t, calls, repl.Calls())
	calls = repl.Calls()
	expected := map[string]bool {"source-namespace/source-name": true}
	for _, key := range beforeKeys {
		expected[key] = true
	}
	found := map[string]bool{}
	for key, _ := range repl.Versions() {
		found[key] = true
		if key == source.Key() {
			continue
		}
		keys := strings.Split(key, "/")
		fake, err := repl.GetFake(keys[0], keys[1])
		if !assert.NoError(t, err, key) || !assert.NotNil(t, fake, key) {
			continue
		}
		assert.Equal(t, source.Data, fake.Data, key)
		atV, atOk := fake.Annotations[ReplicationTimeAnnotation]
		byV, byOk := fake.Annotations[CreatedByAnnotation]
		vV, vOk := fake.Annotations[ReplicatedVersionAnnotation]
		if assert.True(t, atOk, key) {
			IsTimestamp(t, atV, key)
		}
		if assert.True(t, byOk, key) {
			assert.Equal(t, source.Key(), byV, key)
		}
		if assert.True(t, vOk, key) {
			assert.Equal(t, source.ResourceVersion, vV, key)
		}
	}
	assert.Equal(t, expected, found)

	calls += len(beforeKeys)
	source, err = repl.UpdateAddFake(source, "after-data", nil)
	require.NoError(t, err)
	assert.Equal(t, calls, repl.Calls())
	calls = repl.Calls() + len(afterKeys)
	for _, ns := range afterNs {
		require.NoError(t, repl.AddNamespace(ns))
	}
	assert.Equal(t, calls, repl.Calls())
	for _, key := range afterKeys {
		expected[key] = true
	}
	found = map[string]bool{}
	for key, _ := range repl.Versions() {
		found[key] = true
		if key == source.Key() {
			continue
		}
		keys := strings.Split(key, "/")
		fake, err := repl.GetFake(keys[0], keys[1])
		if !assert.NoError(t, err, key) || !assert.NotNil(t, fake, key) {
			continue
		}
		assert.Equal(t, source.Data, fake.Data, key)
		atV, atOk := fake.Annotations[ReplicationTimeAnnotation]
		byV, byOk := fake.Annotations[CreatedByAnnotation]
		vV, vOk := fake.Annotations[ReplicatedVersionAnnotation]
		if assert.True(t, atOk, key) {
			IsTimestamp(t, atV, key)
		}
		if assert.True(t, byOk, key) {
			assert.Equal(t, source.Key(), byV, key)
		}
		if assert.True(t, vOk, key) {
			assert.Equal(t, source.ResourceVersion, vV, key)
		}
	}
	assert.Equal(t, expected, found)
}

// test replicate-to annotation while updated
func TestToAnnotation_AnnotaionsUpdate(t *testing.T) {
	repl := NewFakeReplicator(t, false)
	err := repl.InitNamespaces([]string {"ns1", "ns2", "ns3", "ns4", "ns5"})
	require.NoError(t, err)

	test := func (source *FakeObject) map[string]bool {
		found := map[string]bool {}
		for key, _ := range repl.Versions() {
			if key == source.Key() {
				continue
			}
			found[key] = true
			keys := strings.Split(key, "/")
			fake, err := repl.GetFake(keys[0], keys[1])
			if !assert.NoError(t, err, key) || !assert.NotNil(t, fake, key) {
				continue
			}
			assert.Equal(t, source.Data, fake.Data, key)
			atV, atOk := fake.Annotations[ReplicationTimeAnnotation]
			byV, byOk := fake.Annotations[CreatedByAnnotation]
			vV, vOk := fake.Annotations[ReplicatedVersionAnnotation]
			if assert.True(t, atOk, key) {
				IsTimestamp(t, atV, key)
			}
			if assert.True(t, byOk, key) {
				assert.Equal(t, source.Key(), byV, key)
			}
			if assert.True(t, vOk, key) {
				assert.Equal(t, source.ResourceVersion, vV, key)
			}
		}
		return found
	}

	source := NewFake("source-namespace", "source-name", "data1",
		map[string]string {
			ReplicationTargetsAnnotation: "target-name",
			TargetNamespacesAnnotation: "ns2,ns3,ns5",
		})
	err = repl.SetAddFake(source)
	require.NoError(t, err)
	calls := 3
	assert.Equal(t, calls, repl.Calls())
	calls = repl.Calls()
	expected := map[string]bool {
		"ns2/target-name": true,
		"ns3/target-name": true,
		"ns5/target-name": true,
	}
	found := test(source)
	assert.Equal(t, expected, found)

	source, err = repl.UpdateAddFake(source, "data2", map[string]string {
		ReplicationTargetsAnnotation: "target-name,ns5/other-name",
		TargetNamespacesAnnotation: "ns2,ns4",
	})
	require.NoError(t, err)
	assert.Equal(t, calls+5, repl.Calls())
	calls = repl.Calls()
	expected = map[string]bool {
		"ns2/target-name": true,
		"ns4/target-name": true,
		"ns5/other-name": true,
	}
	found = test(source)
	assert.Equal(t, expected, found)

	source, err = repl.UpdateAddFake(source, "data3", map[string]string {
		ReplicationTargetsAnnotation: "target-name",
		TargetNamespacesAnnotation: "ns[1-4]",
	})
	require.NoError(t, err)
	assert.Equal(t, calls+5, repl.Calls())
	calls = repl.Calls()
	expected = map[string]bool {
		"ns1/target-name": true,
		"ns2/target-name": true,
		"ns3/target-name": true,
		"ns4/target-name": true,
	}
	found = test(source)
	assert.Equal(t, expected, found)
}

// test replicate-to annotation while targets exist
func TestToAnnotation_TargetExists(t *testing.T) {
	repl := NewFakeReplicator(t, false)
	source := NewFake("source-namespace", "source-name", "source-data",
		map[string]string {
			ReplicationTargetsAnnotation: "target-name",
			TargetNamespacesAnnotation: "ns.*",
		})

	test := func (source *FakeObject, expected map[string]bool) {
		found := map[string]bool{}
		for key, _ := range repl.Versions() {
			if key == source.Key() {
				continue
			}
			exp, ok := expected[key]
			found[key] = exp
			if !ok {
				continue
			}
			keys := strings.Split(key, "/")
			if !exp {
				fake, err := repl.GetStoreFake(keys[0], keys[1])
				if assert.NoError(t, err, key) && fake != nil {
					assert.Equal(t, fake.Namespace + "-data", fake.Data, key)
				}
				continue
			}
			fake, err := repl.GetFake(keys[0], keys[1])
			if !assert.NoError(t, err, key) || !assert.NotNil(t, fake, key) {
				continue
			}
			assert.Equal(t, source.Data, fake.Data, key)
			atV, atOk := fake.Annotations[ReplicationTimeAnnotation]
			byV, byOk := fake.Annotations[CreatedByAnnotation]
			vV, vOk := fake.Annotations[ReplicatedVersionAnnotation]
			if assert.True(t, atOk, key) {
				IsTimestamp(t, atV, key)
			}
			if assert.True(t, byOk, key) {
				assert.Equal(t, source.Key(), byV, key)
			}
			if assert.True(t, vOk, key) {
				assert.Equal(t, source.ResourceVersion, vV, key)
			}
		}
		assert.Equal(t, expected, found)
	}
	calls := 0

	require.NoError(t, repl.InitNamespaces([]string {"ns1", "ns2", "ns3"}))
	fake2 := NewFake("ns2", "target-name", "ns2-data", nil)
	fake3 := NewFake("ns3", "target-name", "ns3-data", nil)
	require.NoError(t, repl.SetAddFake(fake2))
	require.NoError(t, repl.SetFake(fake3))
	require.NoError(t, repl.SetAddFake(source))
	assert.Equal(t, calls + 2, repl.Calls())
	calls = repl.Calls()
	test(source, map[string]bool{
		"ns1/target-name": true,
		"ns2/target-name": false,
		"ns3/target-name": false,
	})

	require.NoError(t, repl.UnsetDeleteFake(fake2))
	require.NoError(t, repl.UnsetDeleteFake(fake3))
	fake5 := NewFake("ns5", "target-name", "ns5-data", nil)
	fake6 := NewFake("ns6", "target-name", "ns6-data", nil)
	require.NoError(t, repl.SetAddFake(fake5))
	require.NoError(t, repl.SetFake(fake6))
	require.NoError(t, repl.AddNamespace("ns4"))
	require.NoError(t, repl.AddNamespace("ns5"))
	require.NoError(t, repl.AddNamespace("ns6"))
	assert.Equal(t, calls + 4, repl.Calls())
	calls = repl.Calls()
	test(source, map[string]bool{
		"ns1/target-name": true,
		"ns2/target-name": true,
		"ns3/target-name": true,
		"ns4/target-name": true,
		"ns5/target-name": false,
		"ns6/target-name": false,
	})

	if fake1, err := repl.GetFake("ns1", "target-name");
			assert.NoError(t, err) && assert.NotNil(t, fake1) {
		_, err := repl.UpdateAddFake(fake1, "ns1-data", map[string]string{})
		require.NoError(t, err)
	}
	if fake2, err := repl.GetFake("ns2", "target-name");
			assert.NoError(t, err) && assert.NotNil(t, fake2) {
		require.NoError(t, repl.UnsetDeleteFake(fake2))
	}
	if fake3, err := repl.GetFake("ns3", "target-name");
			assert.NoError(t, err) && assert.NotNil(t, fake3) {
		require.NoError(t, repl.DeleteFake(fake3))
	}
	if fakes, err := repl.DeleteNamespace("ns4");
			assert.NoError(t, err) && assert.Len(t, fakes, 1) &&
			assert.Equal(t, "ns4", fakes[0].Namespace) &&
			assert.Equal(t, "target-name", fakes[0].Name) {
		require.NoError(t, repl.UnsetDeleteFake(fakes[0]))
	}
	assert.Equal(t, calls + 2, repl.Calls())
	calls = repl.Calls()
	test(source, map[string]bool{
		"ns1/target-name": false,
		"ns2/target-name": true,
		"ns3/target-name": false,
		"ns5/target-name": false,
		"ns6/target-name": false,
	})

	require.NoError(t, repl.AddNamespace("ns4"))
	assert.Equal(t, calls + 1, repl.Calls())
	calls = repl.Calls()
	test(source, map[string]bool{
		"ns1/target-name": false,
		"ns2/target-name": true,
		"ns3/target-name": false,
		"ns4/target-name": true,
		"ns5/target-name": false,
		"ns6/target-name": false,
	})

	require.NoError(t, repl.UnsetDeleteFake(source))
	require.NoError(t, repl.AddNamespace("ns7"))
	require.NoError(t, repl.UnsetDeleteFake(fake5))
	require.NoError(t, repl.UnsetDeleteFake(fake6))
	assert.Equal(t, calls + 2, repl.Calls())
	calls = repl.Calls()
	test(source, map[string]bool{
		"ns1/target-name": false,
		"ns3/target-name": false,
	})
}

// test replicate-from annotation while the source or target is updated
func TestFromAnnotation_Updates(t *testing.T) {
	repl := NewFakeReplicator(t, false)
	test := func (source *FakeObject) {
		target, err := repl.GetFake("target-namespace", "target-name")
		if !assert.NoError(t, err) || !assert.NotNil(t, target) {
			return
		}
		atV, atOk := target.Annotations[ReplicationTimeAnnotation]
		vV, vOk := target.Annotations[ReplicatedVersionAnnotation]
		if assert.True(t, atOk) {
			IsTimestamp(t, atV)
		}
		if source != nil {
			assert.Equal(t, source.Data, target.Data)
			if assert.True(t, vOk) {
				assert.Equal(t, source.ResourceVersion, vV)
			}
		} else {
			assert.Equal(t, "", target.Data)
			assert.False(t, vOk)
		}
	}
	calls := 0

	target := NewFake("target-namespace", "target-name", "target-data",
		map[string]string {
			ReplicationSourceAnnotation: "source-namespace/source1",
		})
	require.NoError(t, repl.SetAddFake(target))
	assert.Equal(t, calls, repl.Calls())
	calls = repl.Calls()

	source1 := NewFake("source-namespace", "source1", "data1", nil)
	require.NoError(t, repl.SetAddFake(source1))
	assert.Equal(t, calls, repl.Calls())
	calls = repl.Calls()

	source1, err := repl.UpdateAddFake(source1, "data1", map[string]string {
		ReplicationAllowedAnnotation: "true",
	})
	require.NoError(t, err)
	assert.Equal(t, calls + 1, repl.Calls())
	calls = repl.Calls()
	test(source1)

	source2 := NewFake("source-namespace", "source2", "data2",
		map[string]string {
			ReplicationAllowedAnnotation: "true",
		})
	require.NoError(t, repl.SetAddFake(source2))
	target, err = repl.GetFake("target-namespace", "target-name")
	require.NoError(t, err)
	require.NotNil(t, target)
	_, err = repl.UpdateAddFake(target, "", map[string]string {
		ReplicationSourceAnnotation: "source-namespace/source2",
	})
	require.NoError(t, err)
	assert.Equal(t, calls + 1, repl.Calls())
	calls = repl.Calls()
	test(source2)

	source2, err = repl.UpdateAddFake(source2, "data3", nil)
	require.NoError(t, err)
	assert.Equal(t, calls + 1, repl.Calls())
	calls = repl.Calls()
	test(source2)

	source2, err = repl.UpdateAddFake(source2, "data2", map[string]string {
		ReplicationAllowedAnnotation: "false",
	})
	require.NoError(t, err)
	assert.Equal(t, calls + 1, repl.Calls())
	calls = repl.Calls()
	test(nil)

	target, err = repl.GetFake("target-namespace", "target-name")
	require.NoError(t, err)
	require.NotNil(t, target)
	_, err = repl.UpdateAddFake(target, "", map[string]string {
		ReplicationSourceAnnotation: "source-namespace/source1",
	})
	require.NoError(t, err)
	assert.Equal(t, calls + 1, repl.Calls())
	calls = repl.Calls()
	test(source1)

	require.NoError(t, repl.UnsetDeleteFake(source1))
	assert.Equal(t, calls + 1, repl.Calls())
	calls = repl.Calls()
	test(nil)
}

// test deprecated annotations update
func Test_deprecated_annotations(t *testing.T) {
	previous := AnnotationsPrefix
	deprecated["deprecated-once"] = "replicate-once"
	PrefixAnnotations("test-deprecated/")
	defer func() {
		delete(deprecated, "deprecated-once")
		PrefixAnnotations(previous)
	}()
	examples := []struct{
		name   string
		before map[string]string
		after  map[string]string
	}{{
		"ok",
		map[string]string {
			ReplicationAllowedAnnotation: "true",
		},
		nil,
	},{
		"update",
		map[string]string {
			ReplicationAllowedAnnotation: "true",
			"test-deprecated/deprecated-once": "true",
		},
		map[string]string {
			ReplicationAllowedAnnotation: "true",
			"test-deprecated/replicate-once": "true",
		},
	},{
		"invalid",
		map[string]string {
			ReplicationAllowedAnnotation: "true",
			"test-deprecated/other-annotation": "true",
		},
		nil,
	}}
	for _, example := range examples {
		update := example.after != nil
		if example.after == nil {
			example.after = example.before
		}
		fake := NewFake("target-namespace", "target-name", "target-data", example.before)
		repl := NewFakeReplicator(t, false)
		if !assert.NoError(t, repl.SetAddFake(fake), example.name) {
			continue
		}
		fake, err := repl.GetFake("target-namespace", "target-name")
		if !assert.NoError(t, err, example.name) || !assert.NotNil(t, fake, example.name) {
			continue
		}
		if !assert.NoError(t, repl.SetAddFake(fake), example.name) {
			continue
		}
		fake, err = repl.GetFake("target-namespace", "target-name")
		if !assert.NoError(t, err, example.name) || !assert.NotNil(t, fake, example.name) {
			continue
		}
		after := map[string]string{}
		for k, v := range fake.Annotations {
			after[k] = v
		}
		delete(after, CheckedAnnotation)
		assert.Equal(t, example.after, after, example.name)
		if update {
			assert.Equal(t, 1, repl.Calls(), example.name)
		} else {
			assert.Equal(t, 0, repl.Calls(), example.name)
		}
	}
}
