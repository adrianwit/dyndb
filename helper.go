package dyndb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func getAttributeType(attributeValue *dynamodb.AttributeValue) string {
	if attributeValue.S != nil {
		return "S"
	}
	if attributeValue.N != nil {
		return "N"
	}

	if attributeValue.BOOL != nil {
		return "BOOL"
	}
	if attributeValue.B != nil {
		return "B"
	}
	return "S"
}
