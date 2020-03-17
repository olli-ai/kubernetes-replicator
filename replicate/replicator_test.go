package replicate

import (
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
  log.SetOutput(ioutil.Discard)
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
		name:       "allow",
		replicated: true,
		annotations: map[string]string {
			ReplicationAllowed: "true",
		},
	}, {
		name:       "allow same namespace",
		replicated: true,
		annotations: map[string]string {
			ReplicationAllowed: "true",
		},
		namespace:  "source-namespace",
		from:       "source-name",
	}, {
		name:       "disallow",
		replicated: false,
		annotations: map[string]string {
			ReplicationAllowed: "false",
		},
	}, {
		name:       "allow all but disallow",
		replicated: false,
		allowAll:   true,
		annotations: map[string]string {
			ReplicationAllowed: "false",
		},
	}, {
		name:       "allow wrong format",
		replicated: false,
		annotations: map[string]string {
			ReplicationAllowed: "other",
		},
	}, {
		name:       "allow all but allow wrong format",
		replicated: false,
		allowAll:   true,
		annotations: map[string]string {
			ReplicationAllowed: "other",
		},
	}, {
		name:       "allow namespace",
		replicated: true,
		annotations: map[string]string {
			ReplicationAllowedNamespaces: "target-namespace",
		},
	}, {
		name:       "allow other namespace",
		replicated: false,
		annotations: map[string]string {
			ReplicationAllowedNamespaces: "other-namespace",
		},
	}, {
		name:       "allow all but allow other namespace",
		replicated: false,
		allowAll:   true,
		annotations: map[string]string {
			ReplicationAllowedNamespaces: "other-namespace",
		},
	}, {
		name:       "allow namespace list",
		replicated: true,
		annotations: map[string]string {
			ReplicationAllowedNamespaces: "first-namespace,target-namespace,last-namespace",
		},
	}, {
		name:       "allow namespace pattern",
		replicated: true,
		annotations: map[string]string {
			ReplicationAllowedNamespaces: "target-.*",
		},
	}, {
		name:       "allow other pattern",
		replicated: false,
		annotations: map[string]string {
			ReplicationAllowedNamespaces: "other-.*",
		},
	}, {
		name:       "allow all but allow other pattern",
		replicated: false,
		allowAll:   true,
		annotations: map[string]string {
			ReplicationAllowedNamespaces: "other-.*",
		},
	}, {
		name:       "allow namespace pattern list",
		replicated: true,
		annotations: map[string]string {
			ReplicationAllowedNamespaces: "first-.*,target-.*,last-.*",
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
					ReplicateFromAnnotation: example.from,
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
			atV, atOk := target.Annotations[ReplicatedAtAnnotation]
			vV, vOk := target.Annotations[ReplicatedFromVersionAnnotation]
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
			atV, atOk := target.Annotations[ReplicatedAtAnnotation]
			_, vOk := target.Annotations[ReplicatedFromVersionAnnotation]
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
		repl := NewFakeReplicator(example.allowAll)
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
		repl = NewFakeReplicator(example.allowAll)
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
			ReplicateToAnnotation: "target-namespace/target-name",
		},
	},{
		testName:    "no namespace (to namespace annotation)",
		annotations: map[string]string {
			ReplicateToNamespacesAnnotation: "target-namespace",
		},
	},{
		testName:    "same namespace",
		name:        "target-name",
		annotations: map[string]string {
			ReplicateToAnnotation: "target-name",
		},
	},{
		testName:    "same name",
		name:        "source-name",
		annotations: map[string]string {
			ReplicateToNamespacesAnnotation: "target-namespace",
		},
		namespace:   "target-namespace",
	},{
		testName:    "to annotation",
		name:        "target-name",
		annotations: map[string]string {
			ReplicateToAnnotation: "target-namespace/target-name",
		},
		namespace:   "target-namespace",
	},{
		testName:    "both annotations",
		name:        "target-name",
		annotations: map[string]string {
			ReplicateToAnnotation: "target-name",
			ReplicateToNamespacesAnnotation: "target-namespace",
		},
		namespace:   "target-namespace",
	},{
		testName:    "pattern to annotations",
		name:        "target-name",
		annotations: map[string]string {
			ReplicateToAnnotation: "target-.*/target-name",
		},
		namespace:   "target-namespace",
	},{
		testName:    "pattern to namespace annotations",
		name:        "source-name",
		annotations: map[string]string {
			ReplicateToNamespacesAnnotation: "target-.*",
		},
		namespace:   "target-namespace",
	},{
		testName:    "pattern both annotations",
		name:        "target-name",
		annotations: map[string]string {
			ReplicateToAnnotation: "target-name",
			ReplicateToNamespacesAnnotation: "target-.*",
		},
		namespace:   "target-namespace",
	},{
		testName:    "list to annotation",
		name:        "target-name",
		annotations: map[string]string {
			ReplicateToAnnotation: "first-namespace/first-name,target-namespace/target-name,last-namespace/last-name",
		},
		namespace:   "target-namespace",
	},{
		testName:    "list to namespace annotation",
		name:        "source-name",
		annotations: map[string]string {
			ReplicateToNamespacesAnnotation: "first-namespace,target-namespace,last-namespace",
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
				atV, atOk := target.Annotations[ReplicatedAtAnnotation]
				byV, byOk := target.Annotations[ReplicatedByAnnotation]
				vV, vOk := target.Annotations[ReplicatedFromVersionAnnotation]
				assert.Equal(t, "source-data", target.Data, example.testName)
				if assert.True(t, atOk, example.testName) {
					IsTimestamp(t, atV, example.name)
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
				atV, atOk = target.Annotations[ReplicatedAtAnnotation]
				byV, byOk = target.Annotations[ReplicatedByAnnotation]
				vV, vOk = target.Annotations[ReplicatedFromVersionAnnotation]
				assert.Equal(t, "source-data", target.Data, example.testName)
				if assert.True(t, atOk, example.testName) {
					IsTimestamp(t, atV, example.name)
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
			assert.Equal(t, calls, repl.Calls(), example.name)
			return true
		}
		// try in different orders
		repl := NewFakeReplicator(false)
		assert.True(t,
			source(repl) &&
			target(repl) &&
			test(repl) &&
			clear(repl) &&
			source(repl) &&
			test(repl),
			example.testName)
		repl = NewFakeReplicator(false)
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
			ReplicateFromAnnotation: "source-namespace/source-name",
			ReplicateToAnnotation: "target-namespace/target-name",
		},
		targetName:      "target-name",
	},{
		name:            "from annotation, --allow-all",
		replicated:      true,
		allowAll:        true,
		middle:          map[string]string{
			ReplicateFromAnnotation: "source-namespace/source-name",
			ReplicateToAnnotation: "target-namespace/target-name",
		},
		targetName:      "target-name",
	},{
		name:            "from annotation, allowed",
		replicated:      true,
		source:          map[string]string{
			ReplicationAllowed: "true",
		},
		middle:          map[string]string{
			ReplicateFromAnnotation: "source-namespace/source-name",
			ReplicateToAnnotation: "target-namespace/target-name",
		},
		targetName:      "target-name",
	},{
		name:            "from annotation, allowed namespace",
		replicated:      true,
		source:          map[string]string{
			ReplicationAllowedNamespaces: "target-namespace",
		},
		middle:          map[string]string{
			ReplicateFromAnnotation: "source-namespace/source-name",
			ReplicateToAnnotation: "target-namespace/target-name",
		},
		targetName:      "target-name",
	},{
		name:            "from annotation, allowed middle",
		replicated:      false,
		source:          map[string]string{
			ReplicationAllowedNamespaces: "niddle-namespace",
		},
		middle:          map[string]string{
			ReplicateFromAnnotation: "source-namespace/source-name",
			ReplicateToAnnotation: "target-namespace/target-name",
		},
		targetName:      "target-name",
	},{
		name:            "from annotation, same namespace",
		replicated:      true,
		allowAll:        true,
		middleNamespace: "source-namespace",
		middle:          map[string]string{
			ReplicateFromAnnotation: "source-name",
			ReplicateToAnnotation: "target-namespace/target-name",
		},
		targetName:      "target-name",
	},{
		name:            "to annotation, same namespace",
		replicated:      true,
		allowAll:        true,
		middle:          map[string]string{
			ReplicateFromAnnotation: "source-namespace/source-name",
			ReplicateToAnnotation: "target-name",
		},
		targetName:      "target-name",
		targetNamespace: "middle-namespace",
	},{
		name:            "to annotation, same name",
		replicated:      true,
		allowAll:        true,
		middle:          map[string]string{
			ReplicateFromAnnotation: "source-namespace/source-name",
			ReplicateToNamespacesAnnotation: "target-namespace",
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
			key := example.targetNamespace + "/" + example.targetName
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
			fromV, fromOk := target.Annotations[ReplicateFromAnnotation]
			byV, byOk := target.Annotations[ReplicatedByAnnotation]
			atV, atOk := target.Annotations[ReplicatedAtAnnotation]
			vV, vOk := target.Annotations[ReplicatedFromVersionAnnotation]
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
			fromV, fromOk := target.Annotations[ReplicateFromAnnotation]
			byV, byOk := target.Annotations[ReplicatedByAnnotation]
			atV, atOk := target.Annotations[ReplicatedAtAnnotation]
			_, vOk := target.Annotations[ReplicatedFromVersionAnnotation]
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
		repl := NewFakeReplicator(example.allowAll)
		assert.True(t,
			source(repl) &&
			middle(repl) &&
			target(repl) &&
			test(repl) &&
			clearSource(repl) &&
			source(repl) &&
			test(repl),
			example.name)
		repl = NewFakeReplicator(example.allowAll)
		assert.True(t,
			source(repl) &&
			target(repl) &&
			middle(repl) &&
			test(repl) &&
			clearMiddle(repl) &&
			middle(repl),
			example.name)
		repl = NewFakeReplicator(example.allowAll)
		assert.True(t,
			middle(repl) &&
			source(repl) &&
			target(repl) &&
			test(repl) &&
			clearSource(repl) &&
			source(repl) &&
			test(repl),
			example.name)
		repl = NewFakeReplicator(example.allowAll)
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
		repl = NewFakeReplicator(example.allowAll)
		assert.True(t,
			target(repl) &&
			source(repl) &&
			middle(repl) &&
			test(repl) &&
			clearSource(repl) &&
			source(repl) &&
			test(repl),
			example.name)
		repl = NewFakeReplicator(example.allowAll)
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
