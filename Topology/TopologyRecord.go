package Topology

type TopologyRecord struct {
	SelfID string
	Type string
	NumOfTask int
	ParentID string
}

func NewTopologyRecord(selfID string, type string, numOfTask int, parentID string) (tr *TopologyRecord) {
	tr = &TopologyRecord {
		SelfID: selfID,
		Type: type,
		NumOfTask: numOfTask,
		ParentID: parentID,
	}
	return tr
}
