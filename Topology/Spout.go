package Topology

type Spout struct {
	NextTuple func() map[string]string
} 
