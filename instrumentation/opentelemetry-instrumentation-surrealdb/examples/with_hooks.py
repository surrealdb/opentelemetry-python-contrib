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
Advanced example showing custom hooks for adding attributes to spans.
"""

import asyncio

from opentelemetry import trace
from opentelemetry.instrumentation.surrealdb import SurrealDBInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter


def request_hook(span, message):
    """Add custom attributes before the request is sent."""
    # Add custom metadata
    span.set_attribute("custom.request_id", message.id)
    if hasattr(message, "method"):
        span.set_attribute("custom.operation_type", message.method.value)


def response_hook(span, response):
    """Add custom attributes after receiving a successful response."""
    # Add response metadata
    if isinstance(response, dict):
        if "result" in response:
            span.set_attribute("custom.has_result", True)
        if "error" in response:
            span.set_attribute("custom.has_error", True)


def failed_hook(span, exception):
    """Add custom attributes when an operation fails."""
    span.set_attribute("custom.error_type", type(exception).__name__)
    span.set_attribute("custom.error_message", str(exception))


async def main():
    # Set up OpenTelemetry
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(
        BatchSpanProcessor(ConsoleSpanExporter())
    )
    
    # Instrument SurrealDB with custom hooks
    SurrealDBInstrumentor().instrument(
        request_hook=request_hook,
        response_hook=response_hook,
        failed_hook=failed_hook,
        capture_statement=True,  # Capture query statements
    )
    
    from surrealdb import AsyncSurreal
    
    async with AsyncSurreal("ws://localhost:8000") as db:
        await db.signin({"username": "root", "password": "root"})
        await db.use("test", "test")
        
        # All operations will have custom attributes added by hooks
        user = await db.create("person", {"name": "Bob", "age": 35})
        print(f"Created user with custom tracing: {user}")
        
        result = await db.query("SELECT * FROM person WHERE age > $min_age", {"min_age": 30})
        print(f"Query result with captured statement: {result}")


if __name__ == "__main__":
    asyncio.run(main())
