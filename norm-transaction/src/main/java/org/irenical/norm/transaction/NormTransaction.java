package org.irenical.norm.transaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import org.irenical.norm.transaction.error.NormTransactionException;

public class NormTransaction<INPUT, OUTPUT> {

  private final List<NormOperation<INPUT, OUTPUT>> operations = new CopyOnWriteArrayList<>();

  private NormHook hook;

  private NormConnectionSupplier connectionSupplier;

  public NormTransaction() {
    this(null);
  }

  public NormTransaction(NormConnectionSupplier connectionSupplier) {
    this.connectionSupplier = connectionSupplier;
  }

  public void setConnectionSupplier(NormConnectionSupplier connectionSupplier) {
    this.connectionSupplier = connectionSupplier;
  }

  public NormConnectionSupplier getConnectionSupplier() {
    return connectionSupplier;
  }

  public void setHook(NormHook hook) {
    this.hook = hook;
  }

  public NormHook getHook() {
    return hook;
  }

  public NormTransaction<INPUT, OUTPUT> appendSelect(Function<NormContext<INPUT, OUTPUT>, String> queryBuilder, Function<NormContext<INPUT, OUTPUT>, Iterable<Object>> parametersBuilder, NormOutputReader<INPUT, OUTPUT> outputReader) {
    return appendSelect(null, queryBuilder, parametersBuilder, outputReader);
  }

  public NormTransaction<INPUT, OUTPUT> appendSelect(Function<NormContext<INPUT, OUTPUT>, Boolean> condition, Function<NormContext<INPUT, OUTPUT>, String> queryBuilder, Function<NormContext<INPUT, OUTPUT>, Iterable<Object>> parametersBuilder, NormOutputReader<INPUT, OUTPUT> outputReader) {
    NormSelect<INPUT, OUTPUT> select = new NormSelect<>();
    select.setQueryBuilder(queryBuilder);
    select.setParametersBuilder(parametersBuilder);
    select.setOutputReader(outputReader);
    select.setCondition(condition);
    return appendSelect(select);
  }

  public NormTransaction<INPUT, OUTPUT> appendSelect(NormSelect<INPUT, OUTPUT> select) {
    return appendOperation(select);
  }

  public NormTransaction<INPUT, OUTPUT> appendInsert(Function<NormContext<INPUT, OUTPUT>, String> queryBuilder, Function<NormContext<INPUT, OUTPUT>, Iterable<Object>> parametersBuilder, NormOutputReader<INPUT, OUTPUT> outputReader) {
    return appendInsert(null, queryBuilder, parametersBuilder, outputReader);
  }

  public NormTransaction<INPUT, OUTPUT> appendInsert(Function<NormContext<INPUT, OUTPUT>, Boolean> condition, Function<NormContext<INPUT, OUTPUT>, String> queryBuilder, Function<NormContext<INPUT, OUTPUT>, Iterable<Object>> parametersBuilder, NormOutputReader<INPUT, OUTPUT> outputReader) {
    NormInsert<INPUT, OUTPUT> insert = new NormInsert<>();
    insert.setQueryBuilder(queryBuilder);
    insert.setParametersBuilder(parametersBuilder);
    insert.setOutputReader(outputReader);
    insert.setCondition(condition);
    return appendInsert(insert);
  }

  public NormTransaction<INPUT, OUTPUT> appendInsert(NormInsert<INPUT, OUTPUT> insert) {
    return appendOperation(insert);
  }

  public NormTransaction<INPUT, OUTPUT> appendUpdate(Function<NormContext<INPUT, OUTPUT>, String> queryBuilder, Function<NormContext<INPUT, OUTPUT>, Iterable<Object>> parametersBuilder, NormOutputReader<INPUT, OUTPUT> outputReader) {
    return appendUpdate(null, queryBuilder, parametersBuilder, outputReader);
  }

  public NormTransaction<INPUT, OUTPUT> appendUpdate(Function<NormContext<INPUT, OUTPUT>, Boolean> condition, Function<NormContext<INPUT, OUTPUT>, String> queryBuilder, Function<NormContext<INPUT, OUTPUT>, Iterable<Object>> parametersBuilder, NormOutputReader<INPUT, OUTPUT> outputReader) {
    NormUpdate<INPUT, OUTPUT> update = new NormUpdate<>();
    update.setQueryBuilder(queryBuilder);
    update.setParametersBuilder(parametersBuilder);
    update.setOutputReader(outputReader);
    update.setCondition(condition);
    return appendUpdate(update);
  }

  public NormTransaction<INPUT, OUTPUT> appendUpdate(NormUpdate<INPUT, OUTPUT> update) {
    return appendOperation(update);
  }

  public NormTransaction<INPUT, OUTPUT> appendDelete(Function<NormContext<INPUT, OUTPUT>, String> queryBuilder, Function<NormContext<INPUT, OUTPUT>, Iterable<Object>> parametersBuilder, NormOutputReader<INPUT, OUTPUT> outputReader) {
    return appendDelete(null, queryBuilder, parametersBuilder, outputReader);
  }

  public NormTransaction<INPUT, OUTPUT> appendDelete(Function<NormContext<INPUT, OUTPUT>, Boolean> condition, Function<NormContext<INPUT, OUTPUT>, String> queryBuilder, Function<NormContext<INPUT, OUTPUT>, Iterable<Object>> parametersBuilder, NormOutputReader<INPUT, OUTPUT> outputReader) {
    NormDelete<INPUT, OUTPUT> delete = new NormDelete<>();
    delete.setQueryBuilder(queryBuilder);
    delete.setParametersBuilder(parametersBuilder);
    delete.setOutputReader(outputReader);
    delete.setCondition(condition);
    return appendDelete(delete);
  }

  public NormTransaction<INPUT, OUTPUT> appendDelete(NormUpdate<INPUT, OUTPUT> delete) {
    return appendOperation(delete);
  }

  public NormTransaction<INPUT, OUTPUT> appendCallable(Function<NormContext<INPUT, OUTPUT>, String> queryBuilder, Function<NormContext<INPUT, OUTPUT>, Iterable<Object>> parametersBuilder, NormOutputReader<INPUT, OUTPUT> outputReader) {
    return appendCallable(null, queryBuilder, parametersBuilder, outputReader);
  }

  public NormTransaction<INPUT, OUTPUT> appendCallable(Function<NormContext<INPUT, OUTPUT>, Boolean> condition, Function<NormContext<INPUT, OUTPUT>, String> queryBuilder, Function<NormContext<INPUT, OUTPUT>, Iterable<Object>> parametersBuilder, NormOutputReader<INPUT, OUTPUT> outputReader) {
    NormCall<INPUT, OUTPUT> call = new NormCall<>();
    call.setQueryBuilder(queryBuilder);
    call.setParametersBuilder(parametersBuilder);
    call.setOutputReader(outputReader);
    call.setCondition(condition);
    return appendCallable(call);
  }

  public NormTransaction<INPUT, OUTPUT> appendCallable(NormCall<INPUT, OUTPUT> call) {
    return appendOperation(call);
  }

  public NormTransaction<INPUT, OUTPUT> appendOperation(NormOperation<INPUT, OUTPUT> operation) {
    operations.add(operation);
    return this;
  }

  public OUTPUT execute() throws SQLException {
    return execute(connectionSupplier, null);
  }

  public OUTPUT execute(INPUT input) throws SQLException {
    return execute(connectionSupplier, input);
  }

  public OUTPUT execute(NormConnectionSupplier connectionSupplier, INPUT input) throws SQLException {
    if (connectionSupplier == null) {
      throw new NormTransactionException("No connection supplier was provided for this transaction");
    }
    NormContext<INPUT, OUTPUT> context = new NormContext<>(this);
    context.setInput(input);
    if (hook != null) {
      hook.transactionStarted(context);
    }
    Connection connection = connectionSupplier.get();
    if (connection == null) {
      throw new NormTransactionException("Null connection supplied to this transaction");
    }
    context.setConnection(connection);
    try {
      for (NormOperation<INPUT, OUTPUT> operation : new LinkedList<>(operations)) {

        if (operation.condition == null || operation.condition.apply(context)) {

          context.forward(operation);

          if (hook != null) {
            hook.operationStarted(context);
          }
          context.setPreviousOutput(operation.execute(context));
          if (hook != null) {
            hook.operationEnded(context);
          }
        }
      }
      connection.commit();
    } catch (SQLException e) {
      try {
        connection.rollback();
      } catch (SQLException rollbackException) {
        // Error while rolling back... ignoring.
      }
      throw e;
    } finally {
      connection.close();
      if (hook != null) {
        hook.transactionEnded(context);
      }
    }
    return context.getPreviousOutput();
  }

}
