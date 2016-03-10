package cn.yxffcode.mtd.core;

/**
 * Obtain the value of a sql parameter.
 *
 * @author gaohang on 15/12/29.
 */
public interface ParameterSupplier {
  ParameterSupplier NONE = new ParameterSupplier() {
    @Override public int getParameterCount() {
      return 0;
    }

    @Override public Object getParameter(int parameterIndex) {
      return null;
    }

    @Override public Object getParameter(String propertyName) {
      return null;
    }
  };

  int getParameterCount();

  Object getParameter(int parameterIndex);

  Object getParameter(String propertyName);
}
