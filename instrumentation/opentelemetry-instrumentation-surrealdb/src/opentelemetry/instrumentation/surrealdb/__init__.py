# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
The integration with SurrealDB supports the `surrealdb`_ library, it can be
enabled using the ``SurrealDBInstrumentor``.

.. _surrealdb: https://pypi.org/project/surrealdb

Usage
-----

.. code:: python

    from surrealdb import AsyncSurreal
    from opentelemetry.instrumentation.surrealdb import SurrealDBInstrumentor

    SurrealDBInstrumentor().instrument()
    
    async with AsyncSurreal("ws://localhost:8000") as db:
        await db.signin({"username": "root", "password": "root"})
        await db.use("test", "test")
        await db.create("person", {"name": "Alice"})

API
---
The `instrument` method accepts the following keyword args:

* tracer_provider (``TracerProvider``) - an optional tracer provider
* request_hook (``Callable[[Span, RequestMessage], None]``) - a function with extra user-defined logic to be performed before querying surrealdb
* response_hook (``Callable[[Span, dict], None]``) - a function with extra user-defined logic to be performed after the query returns with a successful response
* failed_hook (``Callable[[Span, Exception], None]``) - a function with extra user-defined logic to be performed after the query returns with a failed response
* capture_statement (``bool``) - an optional value to enable capturing the database statement that is being executed

for example:

.. code:: python

    from opentelemetry.instrumentation.surrealdb import SurrealDBInstrumentor
    from surrealdb import AsyncSurreal

    def request_hook(span, message):
        # request hook logic
        pass

    def response_hook(span, response):
        # response hook logic
        pass

    def failed_hook(span, exception):
        # failed hook logic
        pass

    # Instrument surrealdb with hooks
    SurrealDBInstrumentor().instrument(
        request_hook=request_hook,
        response_hook=response_hook,
        failed_hook=failed_hook
    )

    # This will create a span with surrealdb specific attributes
    async with AsyncSurreal("ws://localhost:8000") as db:
        await db.signin({"username": "root", "password": "root"})
        await db.use("test", "test")
        await db.create("person", {"name": "Alice"})
"""

from __future__ import annotations

import asyncio
from functools import wraps
from logging import getLogger
from typing import Any, Callable, Collection

from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.surrealdb.package import _instruments
from opentelemetry.instrumentation.surrealdb.version import __version__
from opentelemetry.instrumentation.utils import is_instrumentation_enabled
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind, Tracer, get_tracer
from opentelemetry.trace.span import Span
from opentelemetry.trace.status import Status, StatusCode

_LOG = getLogger(__name__)

# Sensitive keys that should be scrubbed from attributes
SENSITIVE_KEYS = {"password", "token", "secret", "key", "pass", "auth"}


def _scrub_sensitive_data(data: dict[str, Any]) -> dict[str, Any]:
    """Remove sensitive data from attributes."""
    scrubbed = {}
    for key, value in data.items():
        if any(sensitive in key.lower() for sensitive in SENSITIVE_KEYS):
            scrubbed[key] = "[REDACTED]"
        elif isinstance(value, dict):
            scrubbed[key] = _scrub_sensitive_data(value)
        else:
            scrubbed[key] = value
    return scrubbed


def _set_span_attributes(
    span: Span,
    connection: Any,
    message: Any,
    capture_statement: bool = False,
) -> None:
    """Set span attributes following OpenTelemetry semantic conventions."""
    if not span.is_recording():
        return

    # Required attributes
    span.set_attribute(SpanAttributes.DB_SYSTEM, "surrealdb")
    
    # Get operation name from RequestMessage
    if hasattr(message, "method") and hasattr(message.method, "value"):
        operation = message.method.value
        span.set_attribute(SpanAttributes.DB_OPERATION, operation)
    
    # Connection information
    if hasattr(connection, "host") and connection.host:
        span.set_attribute(SpanAttributes.SERVER_ADDRESS, connection.host)
    
    if hasattr(connection, "port") and connection.port:
        span.set_attribute(SpanAttributes.SERVER_PORT, connection.port)
    
    # Database and namespace
    if hasattr(connection, "database") and connection.database:
        span.set_attribute(SpanAttributes.DB_NAME, connection.database)
    
    if hasattr(connection, "namespace") and connection.namespace:
        span.set_attribute("db.surrealdb.namespace", connection.namespace)
    
    # Connection type
    connection_type = "unknown"
    class_name = connection.__class__.__name__
    if "Http" in class_name:
        connection_type = "http"
    elif "Ws" in class_name:
        connection_type = "websocket"
    elif "Embedded" in class_name:
        connection_type = "embedded"
    span.set_attribute("db.surrealdb.connection_type", connection_type)
    
    # Capture statement for query operations if enabled
    if capture_statement and hasattr(message, "kwargs"):
        query = message.kwargs.get("query")
        if query:
            span.set_attribute(SpanAttributes.DB_STATEMENT, query)


def _instrument_send_method(
    original_send: Callable,
    tracer: Tracer,
    request_hook: Callable[[Span, Any], None] | None,
    response_hook: Callable[[Span, dict], None] | None,
    failed_hook: Callable[[Span, Exception], None] | None,
    capture_statement: bool,
) -> Callable:
    """Wrap the _send method to create spans."""
    
    if asyncio.iscoroutinefunction(original_send):
        @wraps(original_send)
        async def instrumented_send(
            self: Any, message: Any, operation: str, bypass: bool = False
        ) -> dict[str, Any]:
            if not is_instrumentation_enabled():
                return await original_send(self, message, operation, bypass)
            
            # Extract operation name for span
            method = "unknown"
            if hasattr(message, "method") and hasattr(message.method, "value"):
                method = message.method.value
            
            span_name = f"surrealdb.{method}"
            
            with tracer.start_as_current_span(
                span_name,
                kind=SpanKind.CLIENT,
            ) as span:
                # Set span attributes
                _set_span_attributes(span, self, message, capture_statement)
                
                # Execute request hook
                if request_hook:
                    try:
                        request_hook(span, message)
                    except Exception as hook_exception:
                        _LOG.exception(hook_exception)
                
                try:
                    result = await original_send(self, message, operation, bypass)
                    
                    # Set success status
                    span.set_status(Status(StatusCode.OK))
                    
                    # Execute response hook
                    if response_hook:
                        try:
                            response_hook(span, result)
                        except Exception as hook_exception:
                            _LOG.exception(hook_exception)
                    
                    return result
                    
                except Exception as exc:
                    # Record exception
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR, str(exc)))
                    
                    # Execute failed hook
                    if failed_hook:
                        try:
                            failed_hook(span, exc)
                        except Exception as hook_exception:
                            _LOG.exception(hook_exception)
                    
                    raise
        
        return instrumented_send
    else:
        # Sync version
        @wraps(original_send)
        def instrumented_send(
            self: Any, message: Any, operation: str, bypass: bool = False
        ) -> dict[str, Any]:
            if not is_instrumentation_enabled():
                return original_send(self, message, operation, bypass)
            
            # Extract operation name for span
            method = "unknown"
            if hasattr(message, "method") and hasattr(message.method, "value"):
                method = message.method.value
            
            span_name = f"surrealdb.{method}"
            
            with tracer.start_as_current_span(
                span_name,
                kind=SpanKind.CLIENT,
            ) as span:
                # Set span attributes
                _set_span_attributes(span, self, message, capture_statement)
                
                # Execute request hook
                if request_hook:
                    try:
                        request_hook(span, message)
                    except Exception as hook_exception:
                        _LOG.exception(hook_exception)
                
                try:
                    result = original_send(self, message, operation, bypass)
                    
                    # Set success status
                    span.set_status(Status(StatusCode.OK))
                    
                    # Execute response hook
                    if response_hook:
                        try:
                            response_hook(span, result)
                        except Exception as hook_exception:
                            _LOG.exception(hook_exception)
                    
                    return result
                    
                except Exception as exc:
                    # Record exception
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR, str(exc)))
                    
                    # Execute failed hook
                    if failed_hook:
                        try:
                            failed_hook(span, exc)
                        except Exception as hook_exception:
                            _LOG.exception(hook_exception)
                    
                    raise
        
        return instrumented_send


class SurrealDBInstrumentor(BaseInstrumentor):
    """An instrumentor for SurrealDB."""
    
    _original_methods: dict[tuple[type, str], Callable] = {}
    
    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments
    
    def _instrument(self, **kwargs: Any) -> None:
        """Instrument SurrealDB by wrapping the _send method in all connection classes."""
        tracer_provider = kwargs.get("tracer_provider")
        request_hook = kwargs.get("request_hook")
        response_hook = kwargs.get("response_hook")
        failed_hook = kwargs.get("failed_hook")
        capture_statement = kwargs.get("capture_statement", False)
        
        tracer = get_tracer(
            __name__,
            __version__,
            tracer_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )
        
        # Import connection classes
        try:
            from surrealdb.connections.async_http import AsyncHttpSurrealConnection
            from surrealdb.connections.async_ws import AsyncWsSurrealConnection
            from surrealdb.connections.blocking_http import BlockingHttpSurrealConnection
            from surrealdb.connections.blocking_ws import BlockingWsSurrealConnection
            
            connection_classes = [
                AsyncHttpSurrealConnection,
                AsyncWsSurrealConnection,
                BlockingHttpSurrealConnection,
                BlockingWsSurrealConnection,
            ]
            
            # Try to import embedded connections (they may not be available)
            try:
                from surrealdb.connections.async_embedded import AsyncEmbeddedSurrealConnection
                from surrealdb.connections.blocking_embedded import BlockingEmbeddedSurrealConnection
                connection_classes.extend([
                    AsyncEmbeddedSurrealConnection,
                    BlockingEmbeddedSurrealConnection,
                ])
            except ImportError:
                pass
            
            # Instrument all connection classes
            for connection_class in connection_classes:
                if hasattr(connection_class, "_send"):
                    # Save original method
                    original_send = connection_class._send
                    key = (connection_class, "_send")
                    if key not in self._original_methods:
                        self._original_methods[key] = original_send
                    
                    # Replace with instrumented version
                    instrumented = _instrument_send_method(
                        original_send,
                        tracer,
                        request_hook,
                        response_hook,
                        failed_hook,
                        capture_statement,
                    )
                    connection_class._send = instrumented
                    
        except ImportError as e:
            _LOG.warning(f"Failed to import surrealdb connection classes: {e}")
    
    def _uninstrument(self, **kwargs: Any) -> None:
        """Uninstrument SurrealDB by restoring original methods."""
        for (connection_class, method_name), original_method in self._original_methods.items():
            setattr(connection_class, method_name, original_method)
        self._original_methods.clear()
