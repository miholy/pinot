package com.linkedin.pinot.common.request;

import java.util.List;

public class TransformFunctionField {

  private boolean _isFunction;
  private String _columnName;
  private String _functionName;
  private List<TransformFunctionField> _transformFunctionFields;

  public TransformFunctionField(String functionName, List<TransformFunctionField> transformFunctionFields) {
    this(functionName, transformFunctionFields, null, true);
  }

  public TransformFunctionField(String columnName) {
    this(null, null, columnName, false);
  }

  public TransformFunctionField(String functionName, List<TransformFunctionField> transformFunctionFields, String columnName,
      boolean isFunction) {
    this._functionName = functionName;
    this._transformFunctionFields = transformFunctionFields;
    this._columnName = columnName;
    this._isFunction = isFunction;
  }

  public String getFunctionName() {
    return _functionName;
  }

  public List<TransformFunctionField> getTransformFunctionFields() {
    return _transformFunctionFields;
  }

  public boolean isFunction() {
    return _isFunction;
  }

  public String getColumnName() {
    return _columnName;
  }

  public void setFunctionName(String functionName) {
    this._functionName = functionName;
  }

  public void setTransformFunctionFields(List<TransformFunctionField> transformFunctionFields) {
    this._transformFunctionFields = transformFunctionFields;
  }

  public void setIsFunction(boolean isFunction) {
    this._isFunction = isFunction;
  }

  public void setColumnName(String columnName) {
    this._columnName = columnName;
  }
}
