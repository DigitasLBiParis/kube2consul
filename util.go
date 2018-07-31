package main

import (
	"regexp"
	"strconv"
	"strings"

	"k8s.io/api/core/v1"
)

var (
	knownTags = []string{
		"name",
		"ignore",
		"tags",
	}
)

func isKnownTag(a string) (bool, string) {
	for _, b := range knownTags {
		if strings.HasPrefix(a, b) {
			return true, b
		}
	}
	return false, ""
}

func mapDefault(m map[string]string, key, defaultValue string) string {
	v, ok := m[key]
	if !ok || v == "" {
		return defaultValue
	}
	return v
}

func addMetadata(metadata map[string]string, key, value string) string {
	metadata[key] = value
	return key
}

func serviceMetaData(endpoint *v1.Endpoints, service *v1.Service, port string) (map[string]string, map[string]bool) {
	meta := make([]string, 0)
	for k, v := range service.Annotations {
		meta = append(meta, k+"="+v)
	}
	metadata := make(map[string]string)
	metadata["tags"] = ""
	metadataFromPort := make(map[string]bool)
	for _, kv := range meta {
		kvp := strings.SplitN(kv, "=", 2)
		if strings.HasPrefix(kvp[0], "SERVICE_") && len(kvp) > 1 {
			key := strings.ToLower(strings.TrimPrefix(kvp[0], "SERVICE_"))
			if metadataFromPort[key] {
				continue
			}
			portkey := strings.SplitN(key, "_", 2)
			_, err := strconv.Atoi(portkey[0])
			if err == nil && len(portkey) > 1 {
				isKnown, _ := isKnownTag(portkey[1])
				if !isKnown || portkey[0] != port {
					continue
				}
				usedKey := addMetadata(metadata, portkey[1], kvp[1])
				metadataFromPort[usedKey] = true
			} else if isKnown, _ := isKnownTag(key); isKnown {
				addMetadata(metadata, key, kvp[1])
			}
		}
	}
	return metadata, metadataFromPort
}

func parseAnnotations(annotations map[string]string) (tagsArray []string) {
	for key, value := range annotations {
		tagsArray = append(tagsArray, key+"="+value)
	}
	return
}

func tagsToArray(tags string) []string {
	s := strings.Split(tags, ",")
	var r []string
	for _, str := range s {
		if str != "" {
			r = append(r, strings.TrimSpace(str))
		}
	}
	return r
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func initPerServiceEndpointsFromService(service *v1.Service, perServiceEndpoints map[string][]Endpoint) {
	validServiceNameKey := regexp.MustCompile(`^SERVICE_([0-9]+_)?NAME$`)
	for k, v := range service.Annotations {
		if validServiceNameKey.MatchString(k) {
			perServiceEndpoints[v] = []Endpoint{}
		}
	}
	if !opts.explicit {
		perServiceEndpoints[service.Name] = []Endpoint{}
	}
}

func getStringKeysFromMap(mymap map[string][]Endpoint) []string {
	keys := make([]string, len(mymap))

	i := 0
	for k := range mymap {
		keys[i] = k
		i++
	}
	return keys
}
