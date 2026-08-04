package service

import "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"

type staticTemplateLister struct {
	templates []*v1alpha1.SandboxTemplate
}

func (l staticTemplateLister) List() ([]*v1alpha1.SandboxTemplate, error) {
	return l.templates, nil
}

func (l staticTemplateLister) Get(namespace, name string) (*v1alpha1.SandboxTemplate, error) {
	for _, template := range l.templates {
		if template.Namespace == namespace && template.Name == name {
			return template, nil
		}
	}
	return nil, nil
}
