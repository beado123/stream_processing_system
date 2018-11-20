package Topology

type Bolt struct {
	Execute func(map[string]string) map[string]string 
}
