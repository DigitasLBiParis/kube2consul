package main

import (
	"fmt"

	concurrent "github.com/echaouchna/go-threadpool"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	kapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	kcache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	k8sRestClient rest.Interface
)

func newKubeClient(apiserver string, kubeconfig string) (kubeClient kubernetes.Interface, err error) {
	if kubeconfig == "" {
		config, err := rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
		// Allow overriding of apiserver if using inClusterConfig
		// (necessary if kube-proxy isn't properly set up).
		if apiserver != "" {
			config.Host = apiserver
		}
		tokenPresent := false
		if len(config.BearerToken) > 0 {
			tokenPresent = true
		}
		glog.V(2).Infof("service account token present: %v", tokenPresent)
		glog.V(2).Infof("service host: %s", config.Host)
		if kubeClient, err = kubernetes.NewForConfig(config); err != nil {
			return nil, err
		}
	} else {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		// if you want to change the loading rules (which files in which order), you can do so here
		loadingRules.ExplicitPath = kubeconfig
		configOverrides := &clientcmd.ConfigOverrides{}
		// if you want to change override values or bind them to flags, there are methods to help you
		kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
		config, err := kubeConfig.ClientConfig()
		//config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
		//config, err := clientcmd.DefaultClientConfig.ClientConfig()
		if err != nil {
			return nil, err
		}
		kubeClient, err = kubernetes.NewForConfig(config)
		if err != nil {
			return nil, err
		}
	}

	// Informers don't seem to do a good job logging error messages when it
	// can't reach the server, making debugging hard. This makes it easier to
	// figure out if apiserver is configured incorrectly.
	_, err = kubeClient.Discovery().ServerVersion()
	if err != nil {
		return nil, fmt.Errorf("ERROR communicating with k8s apiserver: %v", err)
	}
	return kubeClient, nil
}

// Returns a cache.ListWatch that gets all changes to a resourceType.
func createListWatcher(kubeClient kubernetes.Interface, resourceType string) *kcache.ListWatch {
	k8sRestClient = kubeClient.CoreV1().RESTClient()
	return kcache.NewListWatchFromClient(k8sRestClient, resourceType, kapi.NamespaceAll, fields.Everything())
}

func (k2c *kube2consul) handleUpdate(resourceType string, actionType ActionType, obj interface{}) {
	var action concurrent.Action
	switch resourceType {
	case "endpoints":
		{
			if e, ok := obj.(*v1.Endpoints); ok {
				if !stringInSlice(e.Namespace, ExcludedNamespaces) {
					action = concurrent.Action{Name: actionType.value(), Data: e}
				}
			}
		}
	case "services":
		{
			if s, ok := obj.(*v1.Service); ok {
				if !stringInSlice(s.Namespace, ExcludedNamespaces) {
					action = concurrent.Action{Name: actionType.value(), Data: s}
				}
			}
		}
	}
	jobQueue <- action
}

func cleanGarbage() {
	jobQueue <- concurrent.Action{Name: REMOVE_DNS_GARBAGE.value(), Data: nil}
}

func (k2c *kube2consul) watchEndpoints(kubeClient kubernetes.Interface) kcache.Store {
	go cleanGarbage()
	eStore, eController := kcache.NewInformer(
		createListWatcher(kubeClient, "endpoints"),
		&v1.Endpoints{},
		0,
		kcache.ResourceEventHandlerFuncs{
			AddFunc: func(newObj interface{}) {
				go k2c.handleUpdate("endpoints", ADD_OR_UPDATE, newObj)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				go k2c.handleUpdate("endpoints", ADD_OR_UPDATE, newObj)
			},
		},
	)

	_, sController := kcache.NewInformer(
		createListWatcher(kubeClient, "services"),
		&v1.Service{},
		0,
		kcache.ResourceEventHandlerFuncs{
			DeleteFunc: func(obj interface{}) {
				go k2c.handleUpdate("services", DELETE, obj)
			},
		},
	)
	go eController.Run(wait.NeverStop)
	go sController.Run(wait.NeverStop)
	return eStore
}
