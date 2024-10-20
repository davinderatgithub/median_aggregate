#include <postgres.h>
#include <fmgr.h>

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/stratnum.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


#if PG_VERSION_NUM < 120000 || PG_VERSION_NUM >= 130000
#error "Unsupported PostgreSQL version. Use version 12."
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

typedef struct MedianState
{
	Oid			inputTypeId;	/* OID of the input data type */
	int64		count;			/* number of non-null inputs seen */
	int64		allocated;		/* allocated size of values array */
	Datum	   *values;			/* array of input values */
	TypeCacheEntry *typentry;	/* info about the comparison function */
} MedianState;

static MedianState *init_median_state(FunctionCallInfo fcinfo);
static TypeCacheEntry *get_type_comp_method(Oid type_oid);
static int	datum_qsort_compare(const void *a, const void *b, void *arg);
static Datum calculate_median(MedianState *state);
static Datum calculate_average(Oid inputTypeId, Datum left, Datum right);
static void add_input_element_median_state(MedianState *state, Datum newVal);
static void discard_element_median_state(MedianState *state, Datum datum);


PG_FUNCTION_INFO_V1(median_transfn);
PG_FUNCTION_INFO_V1(median_mtransfn);
PG_FUNCTION_INFO_V1(median_finalfn);
PG_FUNCTION_INFO_V1(combine_median_state);
PG_FUNCTION_INFO_V1(serialize_median_state);
PG_FUNCTION_INFO_V1(deserialize_median_state);



/*
 * init_median_state
 *
 * Iniitalize the median state.
 */
static MedianState *
init_median_state(FunctionCallInfo fcinfo)
{
	MemoryContext agg_context;
	MemoryContext old_context;
	MedianState *state;
	Oid			inputTypeId;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_transfn called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);

	/* First call, initialize the state */
	inputTypeId = get_fn_expr_argtype(fcinfo->flinfo, 1);
	if (inputTypeId == InvalidOid)
		elog(ERROR, "could not determine input data type");

	state = (MedianState *) palloc(sizeof(MedianState));
	state->inputTypeId = inputTypeId;
	state->allocated = 8;
	state->count = 0;
	state->values = (Datum *) palloc(state->allocated * sizeof(Datum));
	state->typentry = (TypeCacheEntry *) get_type_comp_method(inputTypeId);

	MemoryContextSwitchTo(old_context);
	return state;
}

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
	MedianState *state;

	state = PG_ARGISNULL(0) ? NULL : (MedianState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = init_median_state(fcinfo);
	else
		state = (MedianState *) PG_GETARG_POINTER(0);

	if (!PG_ARGISNULL(1))
		add_input_element_median_state(state, PG_GETARG_DATUM(1));

	PG_RETURN_POINTER(state);
}

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
Datum
median_finalfn(PG_FUNCTION_ARGS)
{
	MedianState *state;

	state = PG_ARGISNULL(0) ? NULL : (MedianState *) PG_GETARG_POINTER(0);

	if (state == NULL || state->count == 0)
		PG_RETURN_NULL();

	/* Sort the array */
	qsort_arg(state->values, state->count, sizeof(Datum),
			  datum_qsort_compare, state->typentry);

	return calculate_median(state);
}

/*
 * median_mtransfn
 *
 * Inverse state transition function to be used in moving-aggregate mode. This
 * function has the same argument and result types as msfunc, but it is used to
 * remove a value from the current aggregate state, rather than add a value to
 * it.
 */
Datum
median_mtransfn(PG_FUNCTION_ARGS)
{
	MedianState *state;

	state = PG_ARGISNULL(0) ? NULL : (MedianState *) PG_GETARG_POINTER(0);

	/* Should not get here with no state */
	if (state == NULL)
		elog(ERROR, "median_mtransfn called with NULL state");

	if (!PG_ARGISNULL(1))
		discard_element_median_state(state, PG_GETARG_DATUM(1));

	PG_RETURN_POINTER(state);
}

/*
 * combine_median_state
 *
 * An aggregate combine function used to combine two aggregate transition data
 * states into a single transition data state.
 */
Datum
combine_median_state(PG_FUNCTION_ARGS)
{
	MedianState *state1;
	MedianState *state2;
	MemoryContext agg_context;
	MemoryContext old_context;
	int			i = 0;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state1 = PG_ARGISNULL(0) ? NULL : (MedianState *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (MedianState *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	/* manually copy all fields from state2 to state1 */
	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);
		state1 = (MedianState *) palloc0(sizeof(MedianState));
		state1->inputTypeId = state2->inputTypeId;
		state1->count = state2->count;
		state1->allocated = state2->allocated;
		state1->values = palloc(state2->allocated * sizeof(Datum));
		state1->typentry = palloc(sizeof(TypeCacheEntry));
		memcpy(state1->values, state2->values, state2->count * sizeof(Datum));
		memcpy(state1->typentry, state2->typentry, sizeof(TypeCacheEntry));
		MemoryContextSwitchTo(old_context);
		PG_RETURN_POINTER(state1);
	}

	old_context = MemoryContextSwitchTo(agg_context);
	/* only copy datum values */
	for (i = 0; i < state2->count; ++i)
	{
		add_input_element_median_state(state1, state2->values[i]);
	}

	MemoryContextSwitchTo(old_context);
	PG_RETURN_POINTER(state1);
}

/*
 * serialize_median_state
 *		Serialize MedianState into bytea
 *
 * First we serialize the fixed length fields, and then we traverse each datum
 * based on the datatype we serialize each datum. Before each Datum, serialize a
 * single byte (char) indicating whether the datum is NULL (1) or not (0).
 * TODO: For large datasets, a more efficient null representation (like a
 * bitmap) might be preferable.
 *
 * We do not seralize TypeCacheEntry as this can be genrated during
 * deserialization.
 */
Datum
serialize_median_state(PG_FUNCTION_ARGS)
{
	MedianState *state = PG_ARGISNULL(0) ? NULL : (MedianState *) PG_GETARG_POINTER(0);
	bool		typbyval;
	char		typalign;
	int16		typlen;
	bool		is_varlena;
	StringInfoData buf;
	bytea	   *result;

	/* Check if the input state is NULL */
	if (state == NULL)
		PG_RETURN_NULL();

	/* Ensure we disallow calling when not in aggregate context */
	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	/* Get type information */
	get_typlenbyvalalign(state->inputTypeId, &typlen, &typbyval, &typalign);
	is_varlena = (typlen == -1 || typlen == -2);

	/* Initialize a dynamic buffer using StringInfo */
	initStringInfo(&buf);

	/* Serialize fixed fields: inputTypeId, count, allocated */
	appendBinaryStringInfo(&buf, (char *) &(state->inputTypeId), sizeof(Oid));
	appendBinaryStringInfo(&buf, (char *) &(state->count), sizeof(int64));
	appendBinaryStringInfo(&buf, (char *) &(state->allocated), sizeof(int64));

	/* Serialize each Datum with null flags */
	for (int i = 0; i < state->count; i++)
	{
		/* Determine if the Datum is NULL */
		bool		is_null = (state->values[i] == (Datum) 0);

		/* Serialize the null flag (1 byte) */
		char		null_flag = is_null ? 1 : 0;

		appendBinaryStringInfo(&buf, &null_flag, sizeof(char));

		if (is_null)
		{
			/* No need to serialize the value */
			continue;
		}

		if (typbyval && !is_varlena)
		{
			/* Fixed-size, passed by value */
			appendBinaryStringInfo(&buf, (char *) &(state->values[i]), sizeof(Datum));
		}
		else
		{
			if (is_varlena)
			{
				/* Serialize the length first */
				int32		data_length = VARSIZE_ANY(state->values[i]);

				appendBinaryStringInfo(&buf, (char *) &data_length, sizeof(int32));

				/* Serialize the actual data bytes */
				appendBinaryStringInfo(&buf, (char *) state->values[i], data_length);
			}
			else
			{
				/* Fixed-length types but passed by reference */
				appendBinaryStringInfo(&buf, (char *) &(state->values[i]), sizeof(Datum));
			}
		}
	}
	/* Allocate a bytea with the buffer's content */
	result = (bytea *) palloc(buf.len + VARHDRSZ);
	SET_VARSIZE(result, buf.len + VARHDRSZ);
	memcpy(VARDATA(result), buf.data, buf.len);
	pfree(buf.data);

	/* Free the buffer allocated by StringInfo */
	PG_RETURN_BYTEA_P(result);
}

/*
 * deserialize_median_state
 *		Deserialize the median state from bytea.
 *
 * Here Variable-Length and Null values are handled as well.
 * During deserialization, read the NULL flag first. If set, assign NULL to the
 * corresponding Datum without attempting to read further.
 */
Datum
deserialize_median_state(PG_FUNCTION_ARGS)
{
	bytea	   *state_bytes = PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0);
	MedianState *state;
	char	   *p;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	bool		is_varlena;
	MemoryContext agg_context;
	MemoryContext old_context;

	/* Check if the input bytea is NULL */
	if (state_bytes == NULL)
		PG_RETURN_NULL();

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);
	p = VARDATA(state_bytes);
	state = (MedianState *) palloc(sizeof(MedianState));

	/* Deserialize inputTypeId */
	memcpy(&(state->inputTypeId), p, sizeof(Oid));
	p += sizeof(Oid);

	/* Deserialize count */
	memcpy(&(state->count), p, sizeof(int64));
	p += sizeof(int64);

	/* Deserialize allocated */
	memcpy(&(state->allocated), p, sizeof(int64));
	p += sizeof(int64);

	state->values = (Datum *) palloc0(state->allocated * sizeof(Datum));

	/* Fetch type information */
	get_typlenbyvalalign(state->inputTypeId, &typlen, &typbyval, &typalign);
	is_varlena = (typlen == -1 || typlen == -2);

	/* Allocate memory for values */
	state->values = (Datum *) palloc(state->allocated * sizeof(Datum));

	/* Deserialize each Datum with null flags */
	for (int i = 0; i < state->count; i++)
	{
		/* Deserialize the null flag */
		char		null_flag;

		memcpy(&null_flag, p, sizeof(char));
		p += sizeof(char);

		if (null_flag)
		{
			/* Assign NULL to the Datum */
			state->values[i] = (Datum) 0;
			continue;
		}

		if (typbyval && !is_varlena)
		{
			/* Fixed-size, passed by value */
			Datum		datum;

			memcpy(&datum, p, sizeof(Datum));
			state->values[i] = datum;
			p += sizeof(Datum);
		}
		else
		{
			if (is_varlena)
			{
				/* Deserialize the length first */
				int32		data_length;
				struct varlena *varlena;

				memcpy(&data_length, p, sizeof(int32));
				p += sizeof(int32);

				/* Allocate memory and copy the data bytes */
				varlena = (struct varlena *) palloc(data_length);
				memcpy(varlena, p, data_length);
				p += data_length;

				/* Assign the varlena pointer to Datum */
				state->values[i] = PointerGetDatum(varlena);
			}
			else
			{
				/* Fixed-length types but passed by reference */
				Datum		datum;

				memcpy(&datum, p, sizeof(Datum));
				state->values[i] = datum;
				p += sizeof(Datum);
			}
		}
	}

	/* Reinitialize typentry */
	state->typentry = lookup_type_cache(state->inputTypeId, TYPECACHE_CMP_PROC);

	MemoryContextSwitchTo(old_context);
	PG_RETURN_POINTER(state);
}

/*
 * add_input_element_median_state
 *
 * Add new element to the state.
 */
static void
add_input_element_median_state(MedianState *state, Datum newVal)
{
	state->count++;
	if (state->count > state->allocated - 1)
	{
		state->allocated *= 2;
		state->values = (Datum *) repalloc(state->values, state->allocated * sizeof(Datum));
	}

	state->values[state->count - 1] = newVal;
}

/*
 * discard_element_median_state
 *
 * Remove the first occurance of the given element we find in the state.
 */
static void
discard_element_median_state(MedianState *state, Datum datum)
{
	int			num_elems = state->count;
	Datum	   *elem_values = state->values;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	int			i;
	bool		found = false;

	/* Get element type information */
	get_typlenbyvalalign(state->inputTypeId, &typlen, &typbyval, &typalign);

	/* Copy elements, excluding the first occurrence of the search element */
	for (i = 0; i < num_elems; i++)
	{
		if (!found && datumIsEqual(elem_values[i], datum, typbyval, typlen))
		{
			/* Skip this element (delete the first occurrence) */
			found = true;
			continue;
		}

		if (found && i < num_elems - 1)
		{
			/* Shift elements left to fill the gap */
			elem_values[i] = elem_values[i + 1];
		}
	}

	/* Set the last element to NULL if an element was found and deleted */
	if (found)
	{
		state->count--;
		/* pfree(&elem_values[state->count]); */
	}

}

/*
 * get_type_comp_method
 *
 * Goal is to get the suitable comparison function for the input type_oid.
 *
 * There is a long chain to traverse through as follows:
 *  1. Get the operator class (pg_opclass) for the specified type.
 *      SELECT pg_opclass.oid, pg_opclass.opcfamily WHERE opclass->opcintype == type_id;
 *
 *  2. Using pg_opclass.oid, pg_opclass.opcfamily, and type_oid, get pg_amproc.amproc.
 *
 * As these steps are already done in lookup_type_cache(), we use it here.
 */
static TypeCacheEntry *
get_type_comp_method(Oid type_oid)
{
	TypeCacheEntry *typentry;

	/* Lookup the type cache entry for the given type OID. */
	typentry = lookup_type_cache(type_oid,
								 TYPECACHE_CMP_PROC_FINFO);

	/* Check if the type has a valid comparison function. */
	if (!OidIsValid(typentry->cmp_proc_finfo.fn_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify a comparison function for type %s",
						format_type_be(type_oid))));

	return typentry;
}


/* datum_qsort_compare
 *  Comparison function for qsort
 *
 * We use the element type's default btree opclass, and its default collation
 * if the type is collation-sensitive.
 */
static int
datum_qsort_compare(const void *a, const void *b, void *arg)
{
	Datum		da = *((const Datum *) a);
	Datum		db = *((const Datum *) b);
	TypeCacheEntry *typentry = (TypeCacheEntry *) arg;
	FmgrInfo   *cmpfunc = &typentry->cmp_proc_finfo;
	Datum		result;

	result = FunctionCall2Coll(cmpfunc, typentry->typcollation, da, db);
	return DatumGetInt32(result);
}

/*
 * calculate_median
 *   return median of the sorted array.
 */
static Datum
calculate_median(MedianState *state)
{
	Datum		result;
	int			midpoint = state->count / 2;

	if (state->count % 2 == 0)
	{
		/* Even number of elements, average the two middle values */
		Datum		left = state->values[midpoint - 1];
		Datum		right = state->values[midpoint];

		result = calculate_average(state->inputTypeId, left, right);
	}
	else
	{
		/* Odd number of elements, return the middle value */
		result = state->values[midpoint];
	}

	return result;
}

/*
 * calculate_average
 *
 * For numeric datatypes, if array length is even we are computing the average
 * of two middle values whereas for other datatypes simply using the n/2 nd
 * largest element as median.
 */
static Datum
calculate_average(Oid inputTypeId, Datum left, Datum right)
{
	switch (inputTypeId)
	{
		case INT4OID:
			{
				int32		l = DatumGetInt32(left);
				int32		r = DatumGetInt32(right);

				return Int32GetDatum((l + r) / 2);
			}
		case INT8OID:
			{
				int64		l = DatumGetInt64(left);
				int64		r = DatumGetInt64(right);

				return Int64GetDatum((l + r) / 2);
			}
		case FLOAT4OID:
			{
				float4		l = DatumGetFloat4(left);
				float4		r = DatumGetFloat4(right);

				return Float4GetDatum((l + r) / 2);
			}
		case FLOAT8OID:
			{
				float8		l = DatumGetFloat8(left);
				float8		r = DatumGetFloat8(right);

				return Float8GetDatum((l + r) / 2);
			}
		case NUMERICOID:
			{
				Datum		sum = DirectFunctionCall2(numeric_add, left, right);
				Datum		two = DirectFunctionCall1(int4_numeric, Int32GetDatum(2));

				return DirectFunctionCall2(numeric_div, sum, two);
			}
		default:
			{
				/* For other types, just returning the first value */
				return left;
			}
	}
}
