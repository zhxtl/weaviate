//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rerankall

import (
	"fmt"

	"github.com/tailor-inc/graphql"
)

func (p *ReRankerAllProvider) additionalReRankerAllField(classname string) *graphql.Field {
	return &graphql.Field{
		Args: graphql.FieldConfigArgument{
			"rankPrompt": &graphql.ArgumentConfig{
				Description:  "Properties which contains text",
				Type:         graphql.String,
				DefaultValue: nil,
			},
			"properties": &graphql.ArgumentConfig{
				Description:  "Property to rank from",
				Type:         graphql.String,
				DefaultValue: nil,
			},
		},
		Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%sAdditionalRerankerAll", classname),
			Fields: graphql.Fields{
				"explanation": &graphql.Field{Type: graphql.String},
			},
		})),
	}
}
