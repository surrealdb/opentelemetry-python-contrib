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
Basic example of SurrealDB instrumentation with OpenTelemetry.

This example demonstrates automatic tracing of SurrealDB operations.
"""

import asyncio

from opentelemetry import trace
from opentelemetry.instrumentation.surrealdb import SurrealDBInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter


async def main():
    # Set up OpenTelemetry
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(
        BatchSpanProcessor(ConsoleSpanExporter())
    )
    
    # Instrument SurrealDB
    SurrealDBInstrumentor().instrument()
    
    # Now all SurrealDB operations will be traced
    from surrealdb import AsyncSurreal
    
    async with AsyncSurreal("ws://localhost:8000") as db:
        # These operations create spans automatically
        await db.signin({"username": "root", "password": "root"})
        await db.use("test", "test")
        
        # CRUD operations
        user = await db.create("person", {"name": "Alice", "age": 30})
        print(f"Created user: {user}")
        
        users = await db.select("person")
        print(f"All users: {users}")
        
        result = await db.query("SELECT * FROM person WHERE age > 25")
        print(f"Query result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
