package util

import (
	"fmt"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	k8metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klog "k8s.io/klog/v2"
)

type UserError struct {
	// Error for internal debugging.
	internalError error
	// Error message for the external client.
	externalMessage string
	// Status code for the external client.
	externalStatusCode codes.Code
}

func newUserError(internalError error, externalMessage string,
	externalStatusCode codes.Code,
) *UserError {
	return &UserError{
		internalError:      internalError,
		externalMessage:    externalMessage,
		externalStatusCode: externalStatusCode,
	}
}

func ExtractErrorForCLI(err error, isDebugMode bool) error {
	// Check if the error is of type *UserError, even if it's wrapped
	var userError *UserError
	if errors.As(err, &userError) {
		if isDebugMode {
			return fmt.Errorf("%+w", userError.internalError)
		}
		return fmt.Errorf("%v", userError.externalMessage)
	}
	return err
}

func NewInternalServerError(err error, internalMessageFormat string,
	a ...any,
) *UserError {
	internalMessage := fmt.Sprintf(internalMessageFormat, a...)
	return newUserError(
		errors.Wrapf(err, "InternalServerError: %v", internalMessage),
		"Internal Server Error",
		codes.Internal)
}

func NewNotFoundError(err error, externalMessageFormat string,
	a ...any,
) *UserError {
	externalMessage := fmt.Sprintf(externalMessageFormat, a...)
	return newUserError(
		errors.Wrapf(err, "NotFoundError: %v", externalMessage),
		externalMessage,
		codes.NotFound)
}

func NewResourceNotFoundError(resourceType string, resourceName string) *UserError {
	externalMessage := fmt.Sprintf("%s %s not found.", resourceType, resourceName)
	return newUserError(
		fmt.Errorf("ResourceNotFoundError: %v", externalMessage),
		externalMessage,
		codes.NotFound)
}

func NewResourcesNotFoundError(resourceTypesFormat string, resourceNames ...any) *UserError {
	externalMessage := fmt.Sprintf("%s not found.", fmt.Sprintf(resourceTypesFormat, resourceNames...))
	return newUserError(
		fmt.Errorf("ResourceNotFoundError: %v", externalMessage),
		externalMessage,
		codes.NotFound)
}

func NewInvalidInputError(messageFormat string, a ...any) *UserError {
	message := fmt.Sprintf(messageFormat, a...)
	return newUserError(errors.Errorf("Invalid input error: %v", message), message, codes.InvalidArgument)
}

func NewInvalidInputErrorWithDetails(err error, externalMessage string) *UserError {
	return newUserError(
		errors.Wrapf(err, "InvalidInputError: %v", externalMessage),
		externalMessage,
		codes.InvalidArgument)
}

func NewAlreadyExistError(messageFormat string, a ...any) *UserError {
	message := fmt.Sprintf(messageFormat, a...)
	return newUserError(errors.Errorf("Already exist error: %v", message), message, codes.AlreadyExists)
}

func NewBadRequestError(err error, externalFormat string, a ...any) *UserError {
	externalMessage := fmt.Sprintf(externalFormat, a...)
	return newUserError(
		errors.Wrapf(err, "BadRequestError: %v", externalMessage),
		externalMessage,
		codes.Aborted)
}

func NewUnauthenticatedError(err error, externalFormat string, a ...any) *UserError {
	externalMessage := fmt.Sprintf(externalFormat, a...)
	return newUserError(
		errors.Wrapf(err, "Unauthenticated: %v", externalMessage),
		externalMessage,
		codes.Unauthenticated)
}

func NewPermissionDeniedError(err error, externalFormat string, a ...any) *UserError {
	externalMessage := fmt.Sprintf(externalFormat, a...)
	return newUserError(
		errors.Wrapf(err, "PermissionDenied: %v", externalMessage),
		externalMessage,
		codes.PermissionDenied)
}

func (e *UserError) ExternalMessage() string {
	return e.externalMessage
}

func (e *UserError) ExternalStatusCode() codes.Code {
	return e.externalStatusCode
}

func (e *UserError) Error() string {
	return e.internalError.Error()
}

func (e *UserError) Cause() error {
	return e.internalError
}

func (e *UserError) String() string {
	return fmt.Sprintf("%v (code: %v): %+v", e.externalMessage, e.externalStatusCode,
		e.internalError)
}

func (e *UserError) ErrorStringWithoutStackTrace() string {
	return fmt.Sprintf("%v: %v", e.externalMessage, e.internalError)
}

// GRPCStatus implements `GRPCStatus` to make sure `FromError` in grpc-go can honor the code.
// Otherwise, it will always return codes.Unknown(2).
// https://github.com/grpc/grpc-go/blob/2c0949c22d46095edc579d9e66edcd025192b98c/status/status.go#L91-L92
func (e *UserError) GRPCStatus() *status.Status {
	return status.New(e.externalStatusCode, e.ErrorStringWithoutStackTrace())
}

func (e *UserError) wrapf(format string, args ...any) *UserError {
	return newUserError(errors.Wrapf(e.internalError, format, args...),
		e.externalMessage, e.externalStatusCode)
}

func (e *UserError) wrap(message string) *UserError {
	return newUserError(errors.Wrap(e.internalError, message),
		e.externalMessage, e.externalStatusCode)
}

func (e *UserError) Log() {
	switch e.externalStatusCode {
	case codes.Aborted, codes.InvalidArgument, codes.NotFound, codes.Internal:
		klog.Infof("%+v", e.internalError)
	default:
		klog.Errorf("%+v", e.internalError)
	}
}

func Wrapf(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}

	var userError *UserError
	if errors.As(err, &userError) {
		return userError.wrapf(format, args...)
	}

	return errors.Wrapf(err, format, args...)
}

func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}

	var userErr *UserError
	if errors.As(err, &userErr) {
		return userErr.wrap(message)
	}

	return errors.Wrapf(err, "%s", message)
}

func LogError(err error) {
	// Check if the error is of type *UserError, even if it's wrapped
	var userError *UserError
	if errors.As(err, &userError) {
		// If it's a *UserError, log it using the Log method
		userError.Log()
	} else {
		// For all other errors, log all the details
		klog.Errorf("InternalError: %+v", err)
	}
}

// TerminateIfError Check if error is nil. Terminate if not.
func TerminateIfError(err error) {
	if err != nil {
		klog.Fatalf("%v", err)
	}
}

// IsNotFound returns whether an error indicates that a resource was "not found".
func IsNotFound(err error) bool {
	return reasonForError(err) == k8metav1.StatusReasonNotFound
}

// IsUserErrorCodeMatch returns whether the error is a user error with specified code.
func IsUserErrorCodeMatch(err error, code codes.Code) bool {
	var userError *UserError
	if errors.As(err, &userError) {
		return userError.externalStatusCode == code
	}
	return false
}

// ReasonForError returns the HTTP status for a particular error.
func reasonForError(err error) k8metav1.StatusReason {
	var statusErr *k8errors.StatusError
	if errors.As(err, &statusErr) {
		return statusErr.Status().Reason
	}

	var apiStatus k8errors.APIStatus
	if errors.As(err, &apiStatus) {
		return apiStatus.Status().Reason
	}

	return k8metav1.StatusReasonUnknown
}
