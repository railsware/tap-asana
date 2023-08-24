from concurrent.futures import ThreadPoolExecutor

from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Tasks(Stream):
    name = "tasks"
    replication_key = "modified_at"
    replication_method = "INCREMENTAL"
    fields = [
        "gid",
        "resource_type",
        "name",
        "approval_status",
        "assignee_status",
        "completed",
        "completed_at",
        "completed_by",
        "created_at",
        "dependencies",
        "dependents",
        "due_at",
        "due_on",
        "external",
        "hearted",
        "hearts",
        "html_notes",
        "is_rendered_as_seperator",
        "liked",
        "likes",
        "memberships",
        "modified_at",
        "notes",
        "num_hearts",
        "num_likes",
        "num_subtasks",
        "resource_subtype",
        "start_on",
        "assignee",
        "custom_fields",
        "followers",
        "parent",
        "permalink_url",
        "projects",
        "tags",
        "workspace",
        "start_at",
        "assignee_section"
    ]

    def __init__(self):
        super().__init__()
        self.session_bookmark = None

    def get_objects(self):
        """Get stream object"""
        # list of project ids
        project_ids = []

        opt_fields = ",".join(self.fields)
        bookmark = self.get_bookmark()
        self.session_bookmark = bookmark
        modified_since = bookmark.strftime("%Y-%m-%dT%H:%M:%S.%f")

        for workspace in self.call_api("workspaces"):
            for project in self.call_api("projects", workspace=workspace["gid"]):
                project_ids.append(project["gid"])

        project_ids_count = len(project_ids)

        if project_ids_count == 0:
            return

        with ThreadPoolExecutor(max_workers=project_ids_count) as executor:
            arguments = [(project_id, opt_fields, modified_since) for project_id in project_ids]
            results = executor.map(self.get_tasks, arguments)

            for result in results:
                for task in result:
                    yield task

        self.update_bookmark(self.session_bookmark)

    def get_tasks(self, params):
        project_id, opt_fields, modified_since = params
        tasks = []

        for task in self.call_api(
                "tasks",
                project=project_id,
                opt_fields=opt_fields,
                modified_since=modified_since,
        ):
            self.session_bookmark = self.get_updated_session_bookmark(
                self.session_bookmark, task[self.replication_key]
            )
            if self.is_bookmark_old(task[self.replication_key]):
                tasks.append(task)

        return tasks


Context.stream_objects["tasks"] = Tasks
