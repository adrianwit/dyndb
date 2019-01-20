package dyndb

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"strings"
)

//getKeyCriteriaMap returns getCriteriaExpression  map
func getKeyCriteriaMap(sqlCriteria *dsc.SQLCriteria, paramIterator toolbox.Iterator) (map[string]interface{}, error) {
	var result = make(map[string]interface{})
	if len(sqlCriteria.Criteria) == 0 {
		return result, nil
	}
	var value interface{}
	if len(sqlCriteria.Criteria) > 1 && sqlCriteria.LogicalOperator != "AND" {
		return nil, fmt.Errorf("only AND logical operator is supported")
	}
	for _, criteria := range sqlCriteria.Criteria {
		if !(criteria.Operator == "=" || criteria.Operator == "IN") {
			return nil, fmt.Errorf("unsuppored operator: %v", criteria.Operator)
		}
		column, ok := criteria.LeftOperand.(string)
		columnValue := criteria.RightOperand.(string)
		if !ok || column == "?" {
			column, ok = criteria.RightOperand.(string)
		}
		if _, has := result[column]; has {
			return nil, fmt.Errorf("invalid getCriteriaExpression: %v", sqlCriteria.Expression())
		}
		bindParamCount := strings.Count(columnValue, "?")
		switch bindParamCount {
		case 0:
			result[column] = strings.Trim(columnValue, "'")
		case 1:
			if !paramIterator.HasNext() {
				return nil, fmt.Errorf("missing bind param: %v %v %v", criteria.LeftOperand, criteria.Operator, criteria.RightOperand)
			}
			if err := paramIterator.Next(&value); err != nil {
				return nil, err
			}
			result[column] = value
			break
		default:
			var values = make([]interface{}, 0)
			for i := 0; i < bindParamCount; i++ {
				if !paramIterator.HasNext() {
					return nil, fmt.Errorf("missing bind param: %v %v %v", criteria.LeftOperand, criteria.Operator, criteria.RightOperand)
				}

				if err := paramIterator.Next(&value); err != nil {
					return nil, err
				}
				values = append(values, value)
			}
			result[column] = values
		}
	}
	return result, nil
}

//processes (key1,key2) IN((), ()) with multi handler call, otherwise calls handler with input getCriteriaExpression values
func processCriteria(criteriaValues map[string]interface{}, handler func(values map[string]interface{}) (bool, error)) (bool, error) {
	multiKey := ""
	var multiValues = make([]interface{}, 0)
	var values = make(map[string]interface{})
	for k, v := range criteriaValues {
		if strings.Count(k, ",") > 0 && toolbox.IsSlice(v) {
			multiKey = k
			multiValues = toolbox.AsSlice(v)
			continue
		}
		values[k] = v
	}
	if multiKey == "" {
		return handler(criteriaValues)
	}
	multiKeys := strings.Split(strings.Trim(multiKey, "()"), ",")
	for i := 0; i < len(multiValues); i += len(multiKeys) {
		for j := 0; j < len(multiKeys); j++ {
			values[strings.TrimSpace(multiKeys[j])] = multiValues[i+j]
		}
		if ok, err := handler(values); err != nil || !ok {
			return ok, err
		}

	}
	return true, nil
}

//getCriteriaExpression returns expression with expression attributes or error
func getCriteriaExpression(sqlCriteria *dsc.SQLCriteria, paramIterator toolbox.Iterator) (*string, map[string]*dynamodb.AttributeValue, error) {
	criteriaExpression := sqlCriteria.Expression()
	values := make(map[string]interface{})
	var i = 1
	for paramIterator.HasNext() {
		var value interface{}
		if err := paramIterator.Next(&value); err != nil {
			return nil, nil, err
		}
		namedParam := fmt.Sprintf(":p%v", i)
		criteriaExpression = strings.Replace(criteriaExpression, "?", namedParam, 1)
		values[namedParam] = value
		i++
	}
	criteriaAttributes, err := dynamodbattribute.MarshalMap(values)
	if err != nil {
		return nil, nil, err
	}
	return &criteriaExpression, criteriaAttributes, nil
}
