package Topology

type Topology struct {
	Records []TopologyRecord
}

func NewTopology() (t *Topology) {
	t = &Topology {
		Records: make([]TopologyRecord)
	}
}




