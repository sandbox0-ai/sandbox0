package appservice

import (
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

// SandboxAppServiceView adds manager-derived publishability state to a sandbox service.
type SandboxAppServiceView struct {
	managerapi.SandboxAppService
	Publishable     bool     `json:"publishable"`
	PublishBlockers []string `json:"publish_blockers,omitempty"`
	PublicURL       string   `json:"public_url,omitempty"`
}

// SandboxAppServicesHaveResumeRoute reports whether any configured route resumes a sandbox.
func SandboxAppServicesHaveResumeRoute(services []managerapi.SandboxAppService) bool {
	for _, svc := range services {
		for _, route := range svc.Ingress.Routes {
			if route.Resume {
				return true
			}
		}
	}
	return false
}

// SandboxAppDomain returns the public app domain suffix used by sandbox service
// exposure hosts.
func SandboxAppDomain(publicRegionID, publicRootDomain string) string {
	rootDomain := strings.Trim(strings.TrimSpace(publicRootDomain), ".")
	if rootDomain == "" {
		rootDomain = "sandbox0.app"
	}
	regionID := strings.Trim(strings.TrimSpace(publicRegionID), ".")
	if regionID == "" {
		return ""
	}
	return regionID + "." + rootDomain
}

// SandboxAppServiceViewsForExposure adds derived fields that depend on the
// deployment exposure domain.
func SandboxAppServiceViewsForExposure(sandboxID, exposureDomain string, services []managerapi.SandboxAppService) []SandboxAppServiceView {
	if len(services) == 0 {
		return []SandboxAppServiceView{}
	}
	views := make([]SandboxAppServiceView, 0, len(services))
	for _, service := range services {
		blockers := SandboxAppServicePublishBlockers(service)
		views = append(views, SandboxAppServiceView{
			SandboxAppService: service,
			Publishable:       len(blockers) == 0,
			PublishBlockers:   blockers,
			PublicURL:         SandboxAppServicePublicURL(sandboxID, exposureDomain, service),
		})
	}
	return views
}

// SandboxAppServicePublicURL returns the public entrypoint for a service when
// the service is public and the deployment has an exposure domain.
func SandboxAppServicePublicURL(sandboxID, exposureDomain string, service managerapi.SandboxAppService) string {
	if !service.Ingress.Public {
		return ""
	}
	sandboxID = strings.TrimSpace(sandboxID)
	exposureDomain = strings.Trim(strings.TrimSpace(exposureDomain), ".")
	if sandboxID == "" || exposureDomain == "" {
		return ""
	}
	label, err := naming.BuildExposureHostLabel(sandboxID, service.Port)
	if err != nil {
		return ""
	}
	return "https://" + label + "." + exposureDomain
}

// SandboxAppServicePublishBlockers returns reasons why a service cannot be
// published as a function revision.
func SandboxAppServicePublishBlockers(service managerapi.SandboxAppService) []string {
	var blockers []string
	if !service.Ingress.Public || len(service.Ingress.Routes) == 0 {
		blockers = append(blockers, "not_public")
	}
	if service.Runtime == nil {
		blockers = append(blockers, "missing_runtime")
	} else if service.Runtime.Type == managerapi.SandboxAppServiceRuntimeManual {
		blockers = append(blockers, "manual_runtime")
	} else if service.Runtime.Type == managerapi.SandboxAppServiceRuntimeCMD && len(service.Runtime.Command) == 0 {
		blockers = append(blockers, "missing_command")
	} else if service.Runtime.Type == managerapi.SandboxAppServiceRuntimeFunction && service.Runtime.Function == nil {
		blockers = append(blockers, "missing_function")
	}
	return blockers
}
