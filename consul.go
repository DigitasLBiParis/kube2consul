package main

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/golang/glog"
	consulapi "github.com/hashicorp/consul/api"
)

func newConsulClient(consulAPI, consulToken string) (*consulapi.Client, error) {
	config := consulapi.DefaultConfig()
	config.Address = consulAPI
	config.Token = consulToken

	consulClient, err := consulapi.NewClient(config)
	if err != nil {
		return nil, err
	}
	_, err = consulClient.Status().Leader()
	if err != nil {
		return nil, fmt.Errorf("ERROR communicating with consul server: %v", err)
	}
	return consulClient, nil
}

func (k2c *kube2consul) registerEndpoint(id int, e Endpoint) error {
	if e.RefName == "" {
		return nil
	}
	debug("[job: %d] %s >>>>>>>>>>>>>>>>>>>> registerEndpoint", id, e.RefName)
	e = updateEndpointsRefNames([]Endpoint{e})[0]

	consulServices, _, err := k2c.consulCatalog.Service(e.Name, opts.consulTag, nil)
	if err != nil {
		return fmt.Errorf("[job: %d] Failed to get services: %v", id, err)
	}

	for _, service := range consulServices {
		if endpointExistsCheckTags(id, service.Node, service.Address, service.ServicePort, service.ServiceTags, []Endpoint{e}, true) {
			debug("[job: %d] %s <<<<<<<<<<<<<<<<<<<<", id, service.Node)
			return nil
		}
	}

	service := &consulapi.AgentService{
		Service: e.Name,
		Port:    int(e.Port),
		Address: e.Address,
		Tags:    append([]string{opts.consulTag}, e.Tags...),
	}

	reg := &consulapi.CatalogRegistration{
		Node:    e.RefName,
		Address: e.Address,
		Service: service,
	}

	_, err = k2c.consulCatalog.Register(reg, nil)
	if err != nil {
		return fmt.Errorf("[job: %d] Error registrating service %v (%v, %v, %v, %v): %v", id, e.Name, e.RefName, e.Address, e.Port, e.Tags, err)
	}
	glog.Infof("[job: %d] Update service %v (%v, %v, %v, %+v)", id, e.Name, e.RefName, e.Address, e.Port, e.Tags)

	debug("[job: %d] %s <<<<<<<<<<<<<<<<<<<<", id, e.RefName)
	return nil
}

func endpointExists(id int, refName, address string, port int, endpoints []Endpoint) bool {
	return endpointExistsCheckTags(id, refName, address, port, nil, endpoints, false)
}

func endpointExistsCheckTags(id int, refName, address string, port int, tags []string, endpoints []Endpoint, checkTags bool) bool {
	for _, e := range endpoints {
		if e.RefName == refName && e.Address == address && int(e.Port) == port {
			if checkTags {
				endpointTags := append([]string{opts.consulTag}, e.Tags...)
				sort.Strings(tags)
				sort.Strings(endpointTags)
				if !reflect.DeepEqual(tags, endpointTags) {
					debug("[job: %d] %s >>> false: different tags", id, refName)
					return false
				}
			}
			debug("[job: %d] %s >>> true: equals", id, refName)
			return true
		}
	}
	debug("[job: %d] %s >>> false: not found", id, refName)
	return false
}

func updateEndpointsRefNames(endpoints []Endpoint) (updatedEndpoints []Endpoint) {
	for _, endpoint := range endpoints {
		refName := endpoint.RefName
		if strings.HasPrefix(refName, "consul") {
			refName = opts.consulTag + "-" + refName
		}
		updatedEndpoints = append(updatedEndpoints, Endpoint{endpoint.Name, endpoint.Address, endpoint.Port, refName, endpoint.Tags})
	}
	return
}

func (k2c *kube2consul) removeDeletedEndpoints(id int, serviceName string, endpoints []Endpoint) error {
	endpoints = updateEndpointsRefNames(endpoints)
	updatedNodes := make(map[string]struct{})
	services, _, err := k2c.consulCatalog.Service(serviceName, opts.consulTag, nil)
	if err != nil {
		return fmt.Errorf("[job: %d] Failed to get services: %v", id, err)
	}

	debug("[job: %d] %s >>>>>>>>>>>>>>>>>>>> removeDeletedEndpoints", id, serviceName)
	for _, service := range services {
		if !endpointExists(id, service.Node, service.Address, service.ServicePort, endpoints) {
			dereg := &consulapi.CatalogDeregistration{
				Node:      service.Node,
				Address:   service.Address,
				ServiceID: service.ServiceID,
			}
			_, err := k2c.consulCatalog.Deregister(dereg, nil)
			if err != nil {
				return fmt.Errorf("[job: %d] Error deregistrating service {node: %s, service: %s, address: %s, port: %d}: %v", id, service.Node, service.ServiceName, service.Address, service.ServicePort, err)
			}
			glog.Infof("[job: %d] Deregister service {node: %s, service: %s, address: %s, port: %d}", id, service.Node, service.ServiceName, service.Address, service.ServicePort)
			updatedNodes[service.Node] = struct{}{}
		}
	}

	// Remove all empty nodes
	for nodeName := range updatedNodes {
		node, _, err := k2c.consulCatalog.Node(nodeName, nil)
		if err != nil {
			return fmt.Errorf("[job: %d] Cannot get node %s: %v", id, nodeName, err)
		} else if node != nil && len(node.Services) == 0 {
			dereg := &consulapi.CatalogDeregistration{
				Node: nodeName,
			}
			_, err = k2c.consulCatalog.Deregister(dereg, nil)
			if err != nil {
				return fmt.Errorf("[job: %d] Error deregistrating node %s: %v", id, nodeName, err)
			}
			glog.Infof("[job: %d] Deregister empty node %s", id, nodeName)
		}
	}
	debug("[job: %d] %s <<<<<<<<<<<<<<<<<<<<", id, serviceName)
	return nil
}

func (k2c *kube2consul) removeDeletedServices(id int, serviceNames []string) error {
	for _, serviceName := range serviceNames {
		k2c.removeDeletedEndpoints(id, serviceName, []Endpoint{})
	}
	return nil
}
