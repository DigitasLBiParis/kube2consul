package main

import (
	"fmt"
	"reflect"
	"sort"

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

func (k2c *kube2consul) registerEndpoint(e Endpoint) error {
	if e.RefName == "" {
		return nil
	}

	consulServices, _, err := k2c.consulCatalog.Service(e.Name, opts.consulTag, nil)
	if err != nil {
		return fmt.Errorf("Failed to get services: %v", err)
	}

	for _, service := range consulServices {
		if endpointExistsCheckTags(service.Node, service.Address, service.ServicePort, service.ServiceTags, []Endpoint{e}, true) {
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
		return fmt.Errorf("Error registrating service %v (%v, %v, %v, %v): %v", e.Name, e.RefName, e.Address, e.Port, e.Tags, err)
	}
	glog.Infof("Update service %v (%v, %v, %v, %+v)", e.Name, e.RefName, e.Address, e.Port, e.Tags)

	return nil
}

func endpointExists(refName, address string, port int, endpoints []Endpoint) bool {
	return endpointExistsCheckTags(refName, address, port, nil, endpoints, false)
}

func endpointExistsCheckTags(refName, address string, port int, tags []string, endpoints []Endpoint, checkTags bool) bool {
	for _, e := range endpoints {
		if e.RefName == refName && e.Address == address && int(e.Port) == port {
			if checkTags {
				endpointTags := append([]string{opts.consulTag}, e.Tags...)
				sort.Strings(tags)
				sort.Strings(endpointTags)
				if !reflect.DeepEqual(tags, endpointTags) {
					return false
				}
			}
			return true
		}
	}
	return false
}

func (k2c *kube2consul) removeDeletedEndpoints(serviceName string, endpoints []Endpoint) error {
	updatedNodes := make(map[string]struct{})
	services, _, err := k2c.consulCatalog.Service(serviceName, opts.consulTag, nil)
	if err != nil {
		return fmt.Errorf("Failed to get services: %v", err)
	}

	for _, service := range services {
		if !endpointExists(service.Node, service.Address, service.ServicePort, endpoints) {
			dereg := &consulapi.CatalogDeregistration{
				Node:      service.Node,
				Address:   service.Address,
				ServiceID: service.ServiceID,
			}
			_, err := k2c.consulCatalog.Deregister(dereg, nil)
			if err != nil {
				return fmt.Errorf("Error deregistrating service {node: %s, service: %s, address: %s, port: %d}: %v", service.Node, service.ServiceName, service.Address, service.ServicePort, err)
			}
			glog.Infof("Deregister service {node: %s, service: %s, address: %s, port: %d}", service.Node, service.ServiceName, service.Address, service.ServicePort)
			updatedNodes[service.Node] = struct{}{}
		}
	}

	// Remove all empty nodes
	for nodeName := range updatedNodes {
		node, _, err := k2c.consulCatalog.Node(nodeName, nil)
		if err != nil {
			return fmt.Errorf("Cannot get node %s: %v", nodeName, err)
		} else if node != nil && len(node.Services) == 0 {
			dereg := &consulapi.CatalogDeregistration{
				Node: nodeName,
			}
			_, err = k2c.consulCatalog.Deregister(dereg, nil)
			if err != nil {
				return fmt.Errorf("Error deregistrating node %s: %v", nodeName, err)
			}
			glog.Infof("Deregister empty node %s", nodeName)
		}
	}
	return nil
}

func (k2c *kube2consul) removeDeletedServices(serviceNames []string) error {
	for _, serviceName := range serviceNames {
		k2c.removeDeletedEndpoints(serviceName, []Endpoint{})
	}
	return nil
}
