package utils

import (
	"fmt"
	"slices"
	"sort"
	"strconv"

	"github.com/emicklei/go-restful/v3"

	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
)

const (
	// Ref: https://github.com/ray-project/ray/blob/ac76c16ebf081bad2bc9b73e952d855dba3fc021/python/ray/util/state/common.py#L48.
	DefaultLimit = 100

	// Ray performs two-stage filtering: the GCS source-side filtering and the API server-side filtering.
	// For the history server, we only mimic the API server-side filtering. So, RayMaxLimitFromDataSource is not used.
	// Ref: https://github.com/ray-project/ray/blob/ac76c16ebf081bad2bc9b73e952d855dba3fc021/python/ray/util/state/common.py#L52-L61.
	RayMaxLimitFromDataSource = 10000
	RayMaxLimitFromAPIServer  = 10000
)

type PredicateType string

const (
	PredicateEqual    PredicateType = "="
	PredicateNotEqual PredicateType = "!="
)

type Filter struct {
	FilterKey       string
	FilterPredicate PredicateType
	FilterValue     string
}

// ListAPIOptions is the legal query parameters for the list API.
// Since the state entities are stored in memory, we don't consider the timeout at this stage.
type ListAPIOptions struct {
	Limit         int
	Timeout       int
	Detail        bool
	ExcludeDriver bool
	Filters       []Filter
}

// ParseOptionsFromReq parses the query parameters from the request and returns the API options for list methods.
// Ref: https://github.com/ray-project/ray/blob/01ac7c99b900a882c3109ba4d99209bef817ceea/python/ray/dashboard/state_api_utils.py#L76-L101.
func ParseOptionsFromReq(req *restful.Request) (ListAPIOptions, error) {
	opts := ListAPIOptions{
		Limit:         DefaultLimit,
		ExcludeDriver: true,
	}

	parse := func(key string, opt any) error {
		val := req.QueryParameter(key)
		if val == "" {
			return nil
		}

		switch t := opt.(type) {
		case *int:
			intVal, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("invalid %s: %w", key, err)
			}
			if intVal < 0 {
				return fmt.Errorf("invalid %s: cannot be negative", key)
			}
			*t = intVal
		case *bool:
			boolVal, err := strconv.ParseBool(val)
			if err != nil {
				return fmt.Errorf("invalid %s: %w", key, err)
			}
			*t = boolVal
		default:
			return fmt.Errorf("unsupported type %T for %s", t, key)
		}
		return nil
	}

	if err := parse("limit", &opts.Limit); err != nil {
		return opts, err
	}
	if opts.Limit > RayMaxLimitFromAPIServer {
		return opts, fmt.Errorf("limit cannot be greater than %d, please use a lower limit", RayMaxLimitFromAPIServer)
	}

	if err := parse("timeout", &opts.Timeout); err != nil {
		return opts, err
	}
	if err := parse("detail", &opts.Detail); err != nil {
		return opts, err
	}
	if err := parse("exclude_driver", &opts.ExcludeDriver); err != nil {
		return opts, err
	}

	filters, err := getFiltersFromReq(req)
	if err != nil {
		return opts, fmt.Errorf("invalid filters parameter: %w", err)
	}
	opts.Filters = filters

	return opts, nil
}

// getFiltersFromReq constructs a slice of filters, each of which is a filter triple (key, predicate, value).
func getFiltersFromReq(req *restful.Request) ([]Filter, error) {
	filterKeys := req.QueryParameters("filter_keys")
	filterPredicates := req.QueryParameters("filter_predicates")
	filterValues := req.QueryParameters("filter_values")
	if len(filterKeys) != len(filterPredicates) || len(filterKeys) != len(filterValues) {
		return nil, fmt.Errorf("filter_keys, filter_predicates, and filter_values must have the same length")
	}

	if len(filterKeys) == 0 {
		return []Filter{}, nil
	}

	filters := make([]Filter, len(filterKeys))
	for i := range filterKeys {
		// TODO(jiangjiawei1103): Add error handling for invalid filter keys based on filterable fields.
		predicate, err := parsePredicate(filterPredicates[i])
		if err != nil {
			return nil, fmt.Errorf("invalid predicate: %w", err)
		}

		filters[i] = Filter{
			FilterKey:       filterKeys[i],
			FilterPredicate: predicate,
			FilterValue:     filterValues[i],
		}
	}

	return filters, nil
}

// parsePredicate converts a string predicate to a PredicateType.
func parsePredicate(predicate string) (PredicateType, error) {
	switch predicate {
	case "=":
		return PredicateEqual, nil
	case "!=":
		return PredicateNotEqual, nil
	default:
		return "", fmt.Errorf("invalid predicate: %s", predicate)
	}
}

// ApplyFilters applies the filters to the tasks and returns the filtered tasks.
// The filtering process follows the following steps:
// 1. Exclude driver tasks
// 2. Apply field-specific filters, including job_id, task_id, actor_id, task_name, and state
// 3. Sort tasks by task_id and task_attempt
// 4. Limit the number of tasks
//
// NOTE: Excluding fields not shown in non-detailed mode is implemented in formatTaskForResponse right before sending back the resp.
// For filtering logic in Ray Dashboard, please refer to:
// https://github.com/ray-project/ray/blob/01ac7c99b900a882c3109ba4d99209bef817ceea/python/ray/util/state/state_manager.py#L231-L294.
func ApplyTaskFilters(tasks []eventtypes.Task, listAPIOptions ListAPIOptions) ([]eventtypes.Task, int) {
	// Exclude driver tasks.
	if listAPIOptions.ExcludeDriver {
		tasks = filterTasks(tasks, Filter{
			FilterKey:       "task_type",
			FilterPredicate: PredicateNotEqual,
			FilterValue:     string(eventtypes.DRIVER_TASK),
		})
	}

	// Apply field-specific filters, including job_id, task_id, actor_id, task_name, and state.
	for _, filter := range listAPIOptions.Filters {
		tasks = filterTasks(tasks, filter)
	}

	numFiltered := len(tasks)
	if numFiltered == 0 {
		return []eventtypes.Task{}, 0
	}

	// The caller's slice may be a shared cached snapshot; never sort it in place.
	tasks = slices.Clone(tasks)
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].TaskID < tasks[j].TaskID || (tasks[i].TaskID == tasks[j].TaskID && tasks[i].TaskAttempt < tasks[j].TaskAttempt)
	})

	// Limit the number of tasks.
	if len(tasks) > listAPIOptions.Limit {
		tasks = tasks[:listAPIOptions.Limit]
	}

	return tasks, numFiltered
}

// filterTasks applies a filter to the tasks and returns the filtered tasks.
func filterTasks(tasks []eventtypes.Task, filter Filter) []eventtypes.Task {
	filteredTasks := make([]eventtypes.Task, 0)
	for _, task := range tasks {
		predicateFunc := PredicateMap[filter.FilterPredicate]

		fieldValue := task.GetFilterableFieldValue(filter.FilterKey)
		if predicateFunc(fieldValue, filter.FilterValue) {
			filteredTasks = append(filteredTasks, task)
		}
	}

	return filteredTasks
}

// TODO(jiangjiawei1103): ApplyFilters and helpers like sortByIdAndAttempt and limitByLimit should be shared among different objects, e.g., actors, nodes.
// The following functions are for compatibility for other endpoints other than tasks.
type PredicateFunc func(fieldValue, filterValue string) bool

var PredicateMap = map[PredicateType]PredicateFunc{
	PredicateEqual:    func(field, value string) bool { return field == value },
	PredicateNotEqual: func(field, value string) bool { return field != value },
}

func ApplyFilter[T any](items []T, filterKey, filterPredicate, filterValue string, fieldGetter func(T, string) string) []T {
	if filterKey == "" || filterValue == "" {
		return items
	}

	predicate, _ := parsePredicate(filterPredicate)
	predicateFunc, ok := PredicateMap[predicate]
	if !ok {
		predicateFunc = PredicateMap[PredicateEqual]
	}

	var result []T
	for _, item := range items {
		fieldValue := fieldGetter(item, filterKey)
		if predicateFunc(fieldValue, filterValue) {
			result = append(result, item)
		}
	}
	return result
}
