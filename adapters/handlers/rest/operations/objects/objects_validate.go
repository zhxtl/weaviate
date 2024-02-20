// Code generated by go-swagger; DO NOT EDIT.

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/entities/models"
)

// ObjectsValidateHandlerFunc turns a function with the right signature into a objects validate handler
type ObjectsValidateHandlerFunc func(ObjectsValidateParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn ObjectsValidateHandlerFunc) Handle(params ObjectsValidateParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// ObjectsValidateHandler interface for that can handle valid objects validate params
type ObjectsValidateHandler interface {
	Handle(ObjectsValidateParams, *models.Principal) middleware.Responder
}

// NewObjectsValidate creates a new http.Handler for the objects validate operation
func NewObjectsValidate(ctx *middleware.Context, handler ObjectsValidateHandler) *ObjectsValidate {
	return &ObjectsValidate{Context: ctx, Handler: handler}
}

/*
	ObjectsValidate swagger:route POST /objects/validate objects objectsValidate

Validate an Object based on a schema.

Validate an Object's schema and meta-data. It has to be based on a schema, which is related to the given Object to be accepted by this validation.
*/
type ObjectsValidate struct {
	Context *middleware.Context
	Handler ObjectsValidateHandler
}

func (o *ObjectsValidate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewObjectsValidateParams()
	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		*r = *aCtx
	}
	var principal *models.Principal
	if uprinc != nil {
		principal = uprinc.(*models.Principal) // this is really a models.Principal, I promise
	}

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params, principal) // actually handle the request
	o.Context.Respond(rw, r, route.Produces, route, res)

}
