package Topology

type TopologyRecord struct {
	SelfID string
	Type string
	NumOfTask int
	ParentID string
	S Spout
	B Bolt
}

func NewTopologyRecord(selfID string, type string, numOfTask int, parentID string, s Spout, b Bolt) (tr *TopologyRecord) {
	tr = &TopologyRecord {
		SelfID: selfID,
		Type: type,
		NumOfTask: numOfTask,
		ParentID: parentID,
		S: s
		B: b
	}
	return tr
}
