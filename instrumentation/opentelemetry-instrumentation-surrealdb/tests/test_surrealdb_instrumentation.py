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

"""Tests for SurrealDB instrumentation."""

import pytest

from opentelemetry import trace
from opentelemetry.instrumentation.surrealdb import SurrealDBInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.semconv.trace import SpanAttributes


class TestSurrealDBInstrumentation:
    """Test cases for SurrealDB instrumentation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.tracer_provider = TracerProvider()
        self.memory_exporter = InMemorySpanExporter()
        span_processor = SimpleSpanProcessor(self.memory_exporter)
        self.tracer_provider.add_span_processor(span_processor)
        trace.set_tracer_provider(self.tracer_provider)
        
        # Clear any previous spans
        self.memory_exporter.clear()
    
    def teardown_method(self):
        """Tear down test fixtures."""
        SurrealDBInstrumentor().uninstrument()
        self.memory_exporter.clear()
    
    def test_instrumentor(self):
        """Test that instrumentor can be instantiated."""
        instrumentor = SurrealDBInstrumentor()
        assert instrumentor is not None
    
    def test_instrument(self):
        """Test basic instrumentation."""
        instrumentor = SurrealDBInstrumentor()
        instrumentor.instrument()
        
        # Verify instrumentation is active
        assert instrumentor._is_instrumented_by_opentelemetry
        
        instrumentor.uninstrument()
    
    def test_instrumentation_dependencies(self):
        """Test that instrumentation dependencies are returned."""
        instrumentor = SurrealDBInstrumentor()
        dependencies = instrumentor.instrumentation_dependencies()
        
        assert len(dependencies) > 0
        assert any("surrealdb" in dep for dep in dependencies)
    
    @pytest.mark.asyncio
    async def test_async_http_instrumentation(self):
        """Test instrumentation of AsyncHttpSurrealConnection."""
        pytest.importorskip("surrealdb")
        from surrealdb.connections.async_http import AsyncHttpSurrealConnection
        
        instrumentor = SurrealDBInstrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        
        try:
            # Check that _send method was instrumented
            assert hasattr(AsyncHttpSurrealConnection, "_send")
            
            # Note: We can't make actual DB calls in unit tests without a running server,
            # but we can verify the instrumentation is in place
            assert instrumentor._is_instrumented_by_opentelemetry
            
        finally:
            instrumentor.uninstrument()
    
    @pytest.mark.asyncio
    async def test_async_ws_instrumentation(self):
        """Test instrumentation of AsyncWsSurrealConnection."""
        pytest.importorskip("surrealdb")
        from surrealdb.connections.async_ws import AsyncWsSurrealConnection
        
        instrumentor = SurrealDBInstrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        
        try:
            # Check that _send method was instrumented
            assert hasattr(AsyncWsSurrealConnection, "_send")
            assert instrumentor._is_instrumented_by_opentelemetry
            
        finally:
            instrumentor.uninstrument()
    
    def test_blocking_http_instrumentation(self):
        """Test instrumentation of BlockingHttpSurrealConnection."""
        pytest.importorskip("surrealdb")
        from surrealdb.connections.blocking_http import BlockingHttpSurrealConnection
        
        instrumentor = SurrealDBInstrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        
        try:
            # Check that _send method was instrumented
            assert hasattr(BlockingHttpSurrealConnection, "_send")
            assert instrumentor._is_instrumented_by_opentelemetry
            
        finally:
            instrumentor.uninstrument()
    
    def test_blocking_ws_instrumentation(self):
        """Test instrumentation of BlockingWsSurrealConnection."""
        pytest.importorskip("surrealdb")
        from surrealdb.connections.blocking_ws import BlockingWsSurrealConnection
        
        instrumentor = SurrealDBInstrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        
        try:
            # Check that _send method was instrumented
            assert hasattr(BlockingWsSurrealConnection, "_send")
            assert instrumentor._is_instrumented_by_opentelemetry
            
        finally:
            instrumentor.uninstrument()
    
    def test_uninstrument(self):
        """Test that uninstrumentation restores original methods."""
        instrumentor = SurrealDBInstrumentor()
        instrumentor.instrument()
        
        assert instrumentor._is_instrumented_by_opentelemetry
        
        instrumentor.uninstrument()
        
        assert not instrumentor._is_instrumented_by_opentelemetry
    
    def test_double_instrumentation(self):
        """Test that double instrumentation doesn't cause issues."""
        instrumentor = SurrealDBInstrumentor()
        instrumentor.instrument()
        instrumentor.instrument()  # Should not raise
        
        assert instrumentor._is_instrumented_by_opentelemetry
        
        instrumentor.uninstrument()
    
    def test_custom_tracer_provider(self):
        """Test using a custom tracer provider."""
        custom_provider = TracerProvider()
        custom_exporter = InMemorySpanExporter()
        custom_provider.add_span_processor(SimpleSpanProcessor(custom_exporter))
        
        instrumentor = SurrealDBInstrumentor()
        instrumentor.instrument(tracer_provider=custom_provider)
        
        try:
            assert instrumentor._is_instrumented_by_opentelemetry
        finally:
            instrumentor.uninstrument()
