//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"bytes"
	"sort"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
)

func (s *Schema) GetClass(className ClassName) *models.Class {
	class, err := GetClassByName(s.Objects, string(className))
	if err != nil {
		return nil
	}

	return class
}

func classBinarySearch(
	classes []*models.Class, className string,
) (int, bool) {
	// Warning: binary search assumes that this array is always sorted, so any
	// write to it must keep that promise.
	cn := []byte(className)
	i := sort.Search(len(classes), func(i int) bool {
		return bytes.Compare([]byte(classes[i].Class), cn) >= 0
	})
	if i < len(classes) && classes[i].Class == string(className) {
		return i, true
	}
	return i, false
}

// FindClassByName will find either a Thing or Class by name.
func (s *Schema) FindClassByName(className ClassName) *models.Class {
	i, ok := classBinarySearch(s.Objects.Classes, string(className))
	if !ok {
		return nil
	}

	return s.Objects.Classes[i]
}

// UpsertClass inserts or replaces the class at the correct position to make
// sure the array stays sorted for binary search
func (s *Schema) UpsertClass(class *models.Class) {
	i, ok := classBinarySearch(s.Objects.Classes, string(class.Class))
	if i == len(s.Objects.Classes) {
		s.Objects.Classes = append(s.Objects.Classes, class)
	}
	if !ok {
		// class does not exist yet, we need to make room for it
		s.Objects.Classes = append(
			s.Objects.Classes[:i+1], s.Objects.Classes[i:]...)
	}
	s.Objects.Classes[i] = class
}

// func (s *Schema) GetKindOfClass(className ClassName) (kind.Kind, bool) {
// 	_, err := GetClassByName(s.Objects, string(className))
// 	if err == nil {
// 		return kind.Object, true
// 	}

// 	return "", false
// }

func PropertyIsIndexed(schemaDefinition *models.Schema, className, tentativePropertyName string) bool {
	propertyName := strings.Split(tentativePropertyName, "^")[0]
	c, err := GetClassByName(schemaDefinition, string(className))
	if err != nil {
		return false
	}
	p, err := GetPropertyByName(c, propertyName)
	if err != nil {
		return false
	}
	indexed := p.IndexInverted
	if indexed == nil {
		return true
	}
	return *indexed
}

func (s *Schema) GetProperty(className ClassName, propName PropertyName) (*models.Property, error) {
	semSchemaClass, err := GetClassByName(s.Objects, string(className))
	if err != nil {
		return nil, err
	}

	semProp, err := GetPropertyByName(semSchemaClass, string(propName))
	if err != nil {
		return nil, err
	}

	return semProp, nil
}

func (s *Schema) GetPropsOfType(propType string) []ClassAndProperty {
	return extractAllOfPropType(s.Objects.Classes, propType)
}

func extractAllOfPropType(classes []*models.Class, propType string) []ClassAndProperty {
	var result []ClassAndProperty
	for _, class := range classes {
		for _, prop := range class.Properties {
			if prop.DataType[0] == propType {
				result = append(result, ClassAndProperty{
					ClassName:    ClassName(class.Class),
					PropertyName: PropertyName(prop.Name),
				})
			}
		}
	}

	return result
}
