package Topology

type Topology struct {
	Records []TopologyRecord
}

func NewTopology() (t *Topology) {
	t = &Topology {
		Records: make([]TopologyRecord)
	}
}

/**
 * Add new record in topology
 * @param record TopologyRecord
 */
func (self *Topology) AddRecord(record TopologyRecord) {
	self.Records = append(self.Records, record)
}




