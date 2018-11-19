package Topology

type TopologyBuilder struct {
	IDList map[string]int
	Topo Topology
}

func NewTopologyBuilder() (tb *TopologyBuilder) {
	tb = &TopologyBuilder {
		IDList: make(map[string]int),
		Topo: new Topology(), 
	}
	return
}

func (self *TopologyBuilder) SetSpout(id string, s Spout) {
	
}

func (self *TopologyBuilder) SetBolt(id string, b Bolt) {

}

func (self *TopologyBuilder) ShuffleGrouping() {

}

func (self *TopologyBuilder) FieldGrouping() {

}



