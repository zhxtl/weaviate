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
	"github.com/tailor-inc/graphql/language/ast"
)

func (p *ReRankerAllProvider) parseReRankerAllArguments(args []*ast.Argument) *Params {
	out := &Params{}

	for _, arg := range args {
		switch arg.Name.Value {
		case "rankPrompt":
			out.RankPrompt = &arg.Value.(*ast.StringValue).Value
		case "properties":
			out.Properties = &arg.Value.(*ast.StringValue).Value
		}
	}

	return out
}
