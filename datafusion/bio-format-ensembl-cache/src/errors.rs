use datafusion::common::DataFusionError;

pub(crate) fn exec_err<M: Into<String>>(message: M) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

pub(crate) type Result<T> = datafusion::common::Result<T>;
