package main

func IdGenerator(initId uint32, idChan chan uint32) {
	for i := uint32(initId); ; i++ {
		idChan <- i
	}
}
