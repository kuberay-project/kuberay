from ray import serve

@serve.deployment()
class BaseService:
    async def __call__(self):
        return "hello world"

DagNode = BaseService.bind()
