package dyndb_test

import (
	"github.com/adrianwit/mgc"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"testing"
)

func TestAsMongoCriteria(t *testing.T) {
	for k := range mgc.OperatorMapping {
		statement := &dsc.BaseStatement{
			Table: "abc",
			SQLCriteria: &dsc.SQLCriteria{
				Criteria: []*dsc.SQLCriterion{
					{
						LeftOperand:  "k1",
						Operator:     k,
						RightOperand: "?",
					},
				},
			},
		}
		criteriaValues, err := mgc.AsMongoCriteria(statement.SQLCriteria, []interface{}{1})
		if assert.Nil(t, err) {
			val, ok := criteriaValues["k1"]
			assert.True(t, ok)
			assert.NotNil(t, val)
		}
	}
}
