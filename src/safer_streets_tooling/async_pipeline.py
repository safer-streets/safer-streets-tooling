import asyncio
from graphlib import TopologicalSorter
from time import time
from typing import Any

from safer_streets_tooling.async_node import AsyncNode
from safer_streets_tooling.result import Result


class AsyncPipeline:
    def __init__(self, *, verbose: bool = False) -> None:
        self.graph = TopologicalSorter()  # type: ignore[var-annotated]
        self.dependencies: dict[str, tuple[str, ...]] = {}
        self.nodes: dict[str, AsyncNode[Any, Any]] = {}
        self.results: dict[str, Result[Any]] = {}
        self.verbose = verbose

    def add(self, node_id: str, functor: AsyncNode[Any, Any]) -> None:
        self.graph.add(node_id, *functor.dependency_ids)
        self.dependencies[node_id] = functor.dependency_ids
        self.nodes[node_id] = functor

    def __getitem__[T](self, node_id: str) -> Result[T]:
        return self.results[node_id]

    async def __call__(self) -> None:
        start = time()

        async def execute_task(node_id: str) -> None:
            dependency_ids = self.dependencies[node_id]
            while not all(d in self.results for d in dependency_ids):
                await asyncio.sleep(0.1)
            if self.verbose:
                print(f"{time() - start:.3f}: {node_id} <- {dependency_ids} executing...")
            kwargs = {d: self.results[d] for d in dependency_ids}
            self.results[node_id] = await self.nodes[node_id](**kwargs)
            if self.verbose:
                print(f"{time() - start:.3f}: {node_id} completed")

        await asyncio.gather(*(execute_task(node_id) for node_id in self.graph.static_order()))
        if self.verbose:
            print(f"{time() - start:.3f}: complete")
