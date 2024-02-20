// Code generated by go-swagger; DO NOT EDIT.

package graphql

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// GraphqlPostOKCode is the HTTP code returned for type GraphqlPostOK
const GraphqlPostOKCode int = 200

/*
GraphqlPostOK Successful query (with select).

swagger:response graphqlPostOK
*/
type GraphqlPostOK struct {

	/*
	  In: Body
	*/
	Payload *models.GraphQLResponse `json:"body,omitempty"`
}

// NewGraphqlPostOK creates GraphqlPostOK with default headers values
func NewGraphqlPostOK() *GraphqlPostOK {

	return &GraphqlPostOK{}
}

// WithPayload adds the payload to the graphql post o k response
func (o *GraphqlPostOK) WithPayload(payload *models.GraphQLResponse) *GraphqlPostOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the graphql post o k response
func (o *GraphqlPostOK) SetPayload(payload *models.GraphQLResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GraphqlPostOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// GraphqlPostUnauthorizedCode is the HTTP code returned for type GraphqlPostUnauthorized
const GraphqlPostUnauthorizedCode int = 401

/*
GraphqlPostUnauthorized Unauthorized or invalid credentials.

swagger:response graphqlPostUnauthorized
*/
type GraphqlPostUnauthorized struct {
}

// NewGraphqlPostUnauthorized creates GraphqlPostUnauthorized with default headers values
func NewGraphqlPostUnauthorized() *GraphqlPostUnauthorized {

	return &GraphqlPostUnauthorized{}
}

// WriteResponse to the client
func (o *GraphqlPostUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// GraphqlPostForbiddenCode is the HTTP code returned for type GraphqlPostForbidden
const GraphqlPostForbiddenCode int = 403

/*
GraphqlPostForbidden Forbidden

swagger:response graphqlPostForbidden
*/
type GraphqlPostForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewGraphqlPostForbidden creates GraphqlPostForbidden with default headers values
func NewGraphqlPostForbidden() *GraphqlPostForbidden {

	return &GraphqlPostForbidden{}
}

// WithPayload adds the payload to the graphql post forbidden response
func (o *GraphqlPostForbidden) WithPayload(payload *models.ErrorResponse) *GraphqlPostForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the graphql post forbidden response
func (o *GraphqlPostForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GraphqlPostForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// GraphqlPostUnprocessableEntityCode is the HTTP code returned for type GraphqlPostUnprocessableEntity
const GraphqlPostUnprocessableEntityCode int = 422

/*
GraphqlPostUnprocessableEntity Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response graphqlPostUnprocessableEntity
*/
type GraphqlPostUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewGraphqlPostUnprocessableEntity creates GraphqlPostUnprocessableEntity with default headers values
func NewGraphqlPostUnprocessableEntity() *GraphqlPostUnprocessableEntity {

	return &GraphqlPostUnprocessableEntity{}
}

// WithPayload adds the payload to the graphql post unprocessable entity response
func (o *GraphqlPostUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *GraphqlPostUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the graphql post unprocessable entity response
func (o *GraphqlPostUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GraphqlPostUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// GraphqlPostInternalServerErrorCode is the HTTP code returned for type GraphqlPostInternalServerError
const GraphqlPostInternalServerErrorCode int = 500

/*
GraphqlPostInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response graphqlPostInternalServerError
*/
type GraphqlPostInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewGraphqlPostInternalServerError creates GraphqlPostInternalServerError with default headers values
func NewGraphqlPostInternalServerError() *GraphqlPostInternalServerError {

	return &GraphqlPostInternalServerError{}
}

// WithPayload adds the payload to the graphql post internal server error response
func (o *GraphqlPostInternalServerError) WithPayload(payload *models.ErrorResponse) *GraphqlPostInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the graphql post internal server error response
func (o *GraphqlPostInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GraphqlPostInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
