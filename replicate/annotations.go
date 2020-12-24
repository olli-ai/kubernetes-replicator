package replicate
// Annotations that are used to control this controller's behaviour
var (
	ReplicationSourceAnnotation    = "replicate-from"
	ReplicationTargetsAnnotation   = "replicate-to"
	TargetNamespacesAnnotation     = "replicate-to-namespaces"
	ReplicateOnceAnnotation        = "replicate-once"
	ReplicateOnceVersionAnnotation = "replicate-once-version"
	ReplicationTimeAnnotation      = "replicated-at"
	CreatedByAnnotation            = "replicated-by"
	ReplicatedVersionAnnotation    = "replicated-version"
	ReplicationAllowedAnnotation   = "replication-allowed"
	AllowedNamespacesAnnotation    = "replication-allowed-namespaces"
)

var annotationPointers = map[string]*string {
	"replicate-from":                 &ReplicationSourceAnnotation,
	"replicate-to":                   &ReplicationTargetsAnnotation,
	"replicate-to-namespaces":        &TargetNamespacesAnnotation,
	"replicate-once":                 &ReplicateOnceAnnotation,
	"replicate-once-version":         &ReplicateOnceVersionAnnotation,
	"replicated-at":                  &ReplicationTimeAnnotation,
	"replicated-by":                  &CreatedByAnnotation,
	"replicated-version":             &ReplicatedVersionAnnotation,
	"replication-allowed":            &ReplicationAllowedAnnotation,
	"replication-allowed-namespaces": &AllowedNamespacesAnnotation,
}

var AllAnnotations map[string]bool

var CopyLabels = map[string]string {
	"managed-by": "kubernetes-replicator",
}

var deprecated map[string]string = map[string]string {
	"replicated-from-version": "replicated-version",
}

var DeprecatedAnnotations map[string]string

var AnnotationsPrefix = ""

func init() {
	PrefixAnnotations("kubernetes-replicator/")
}

func PrefixAnnotations(prefix string){
	AnnotationsPrefix = prefix
	a := map[string]bool {}
	for name, ptr := range annotationPointers {
		name = prefix + name
		*ptr = name
		a[name] = true
	}
	AllAnnotations = a
	d := map[string]string {}
	for old, new := range deprecated {
		d[prefix + old] = prefix + new
	}
	DeprecatedAnnotations = d
}
