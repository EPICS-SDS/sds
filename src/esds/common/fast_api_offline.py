from fastapi import FastAPI, Request
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.staticfiles import StaticFiles
from starlette.responses import HTMLResponse


class FastAPIOfflineDocs(FastAPI):
    pass

    def __init__(self, doc_cdon_files, *args, **kwargs):
        super(FastAPIOfflineDocs, self).__init__(
            docs_url=None, redoc_url=None, *args, **kwargs
        )

        self.mount(
            self.root_path + "/static",
            StaticFiles(directory=doc_cdon_files),
            name="static",
        )

        async def custom_swagger_ui_html(req: Request) -> HTMLResponse:
            root_path = req.scope.get("root_path", "").rstrip("/")
            openapi_url = root_path + self.openapi_url
            oauth2_redirect_url = self.swagger_ui_oauth2_redirect_url
            if oauth2_redirect_url:
                oauth2_redirect_url = root_path + oauth2_redirect_url
            return get_swagger_ui_html(
                openapi_url=openapi_url,
                title=self.title + " - Swagger UI",
                oauth2_redirect_url=oauth2_redirect_url,
                swagger_js_url="/static/swagger-ui-bundle.js",
                swagger_css_url="/static/swagger-ui.css",
            )

        self.add_route("/docs", custom_swagger_ui_html, include_in_schema=False)

        async def swagger_ui_redirect(req: Request) -> HTMLResponse:
            return get_swagger_ui_oauth2_redirect_html()

        self.add_route(
            self.swagger_ui_oauth2_redirect_url,
            swagger_ui_redirect,
            include_in_schema=False,
        )

        async def redoc_html(req: Request) -> HTMLResponse:
            root_path = req.scope.get("root_path", "").rstrip("/")
            openapi_url = root_path + self.openapi_url
            return get_redoc_html(
                openapi_url=openapi_url,
                title=self.title + " - ReDoc",
                redoc_js_url="/static/redoc.standalone.js",
            )

        self.add_route("/redoc", redoc_html, include_in_schema=False)
