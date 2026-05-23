package worker

import (
	"orchestrator/internal/models"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)


func getSystemMetrics() models.SystemMetrics {

	// RAM
	memoryFree := uint64(0)
	vmStat, err := mem.VirtualMemory()
	if err == nil {
		memoryFree = vmStat.Available / 1024 / 1024 // convert to MB
	}

	// Percentage of CPU free
	cpuFree := 0.0
	percentages, err := cpu.Percent(0, false)
	if err == nil && len(percentages) > 0 {
		usedCPU := percentages[0]
		cpuFree = 100.0 - usedCPU
	}

	return models.SystemMetrics {
		MemoryFree: memoryFree,
		CPUFree: cpuFree,
	}
}