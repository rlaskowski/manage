package manage

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	srvRWMutex = &sync.RWMutex{}
	services   = make([]*ServiceInfo, 0)
)

type ServiceRunner interface {
	// Start all services
	Start() error

	// Stop all services
	Stop() error

	// Creating new service instance
	CreateService() *ServiceInfo
}

type ServiceInfo struct {
	ID        string
	Priority  int
	Intstance ServiceRunner
}

func RegisterService(runner ServiceRunner) error {
	service := runner.CreateService()

	if service.ID == "" {
		return errors.New("no service id")
	}

	srvRWMutex.Lock()
	defer srvRWMutex.Unlock()

	if _, err := GetService(service.ID); err == nil {
		return fmt.Errorf("service %s has been already registered", service.ID)
	}

	services = append(services, service)

	return nil
}

// Returns service type according service ID
func GetService(id string) (*ServiceInfo, error) {
	srvRWMutex.Lock()
	defer srvRWMutex.Unlock()

	position := sort.Search(len(services), func(i int) bool {
		return services[i].ID >= id
	})

	if !(position < len(services) && services[position].ID == id) {
		return nil, fmt.Errorf("service %s not found", id)
	}

	return services[position], nil
}

// Returns service ID according service type
func GetServiceID(i interface{}) string {
	srvRWMutex.Lock()
	defer srvRWMutex.Unlock()

	service, ok := i.(ServiceInfo)
	if !ok {
		return ""
	}

	return service.ID
}

// Setting services by priority starting
func (s *ServiceInfo) setPriority() {
	sort.Slice(services, func(i, j int) bool {
		return services[i].Priority <= services[j].Priority
	})
}

// Starting all services
func (s *ServiceInfo) Start() error {
	s.setPriority()

	for _, v := range services {
		if result := v.Intstance.Start(); result != nil {
			return fmt.Errorf("couldn't start service %s due to: %s", v.ID, result.Error())
		}
	}

	return nil
}

// Stopping all services
func (s *ServiceInfo) Stop() error {
	for _, v := range services {
		if result := v.Intstance.Stop(); result != nil {
			return fmt.Errorf("couldn't start service %s due to: %s", v.ID, result.Error())
		}
	}

	return nil
}
