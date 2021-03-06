package replicate

// Annotations that are used to control this controller's behaviour
var (
	ReplicateFromAnnotation         = "replicate-from"
	ReplicateToAnnotation           = "replicate-to"
	ReplicateToNamespacesAnnotation = "replicate-to-namespaces"
	ReplicateOnceAnnotation         = "replicate-once"
	ReplicateOnceVersionAnnotation  = "replicate-once-version"
	ReplicatedAtAnnotation          = "replicated-at"
	ReplicatedByAnnotation          = "replicated-by"
	ReplicatedFromVersionAnnotation = "replicated-from-version"
	ReplicationAllowed              = "replication-allowed"
	ReplicationAllowedNamespaces    = "replication-allowed-namespaces"
)

func PrefixAnnotations(prefix string){
	ReplicateFromAnnotation         = prefix + ReplicateFromAnnotation
	ReplicateToAnnotation           = prefix + ReplicateToAnnotation
	ReplicateToNamespacesAnnotation = prefix + ReplicateToNamespacesAnnotation
	ReplicateOnceAnnotation         = prefix + ReplicateOnceAnnotation
	ReplicateOnceVersionAnnotation  = prefix + ReplicateOnceVersionAnnotation
	ReplicatedAtAnnotation          = prefix + ReplicatedAtAnnotation
	ReplicatedByAnnotation          = prefix + ReplicatedByAnnotation
	ReplicatedFromVersionAnnotation = prefix + ReplicatedFromVersionAnnotation
	ReplicationAllowed              = prefix + ReplicationAllowed
	ReplicationAllowedNamespaces    = prefix + ReplicationAllowedNamespaces
}
