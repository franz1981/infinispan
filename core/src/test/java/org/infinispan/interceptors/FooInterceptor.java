package org.infinispan.interceptors;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;

public class FooInterceptor extends BaseCustomAsyncInterceptor {

   public volatile boolean putInvoked;

   private String foo;

   public String getFoo() {
      return foo;
   }

   public void setFoo(String foo) {
      this.foo = foo;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      putInvoked = true;
      return super.visitPutKeyValueCommand(ctx, command);
   }
}
