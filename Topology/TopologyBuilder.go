package Topology

type TopologyBuilder struct {
	IDList map[string]int
	Topo Topology //Topo.Records: array of TopologyRecord
}

func NewTopologyBuilder() (tb *TopologyBuilder) {
	tb = &TopologyBuilder {
		IDList: make(map[string]int),
		Topo: new Topology(), 
	}
	return
}

/**
 * add spout to topology
 */
func (self *TopologyBuilder) SetSpout(id string, s Spout, amountOfParallelism int) {
	
}

/**
 * add bolt to topology
 */
func (self *TopologyBuilder) SetBolt(id string, b Bolt, amountOfParallelism int) {

}

/**
 * declare the parentID where the bolt wants to read all the tuples emitted
 */
func (self *TopologyBuilder) ShuffleGrouping(id string, b Bolt, parentID string) {

}

/**
 * 
 */
func (self *TopologyBuilder) FieldGrouping(id string, b Bolt, parentID string, field string) {

}
