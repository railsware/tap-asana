from concurrent.futures import ThreadPoolExecutor

from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Stories(Stream):
    name = "stories"
    replication_method = "INCREMENTAL"
    replication_key = "created_at"

    fields = [
        "gid",
        "resource_type",
        "created_at",
        "created_by",
        "resource_subtype",
        "text",
        "html_text",
        "is_pinned",
        "assignee",
        "dependency",
        "duplicate_of",
        "duplicated_from",
        "follower",
        "hearted",
        "hearts",
        "is_edited",
        "liked",
        "likes",
        "new_approval_status",
        "new_dates",
        "new_enum_value",
        "old_date_value",
        "new_date_value",
        "old_people_value",
        "new_people_value",
        "new_name",
        "new_number_value",
        "new_resource_subtype",
        "new_section",
        "new_text_value",
        "num_hearts",
        "num_likes",
        "old_approval_status",
        "old_dates",
        "old_enum_value",
        "old_name",
        "old_number_value",
        "old_resource_subtype",
        "old_section",
        "old_text_value",
        "preview",
        "project",
        "source",
        "story",
        "tag",
        "target",
        "task",
        "sticker_name",
        "custom_field",
        "is_editable",
        "new_multi_enum_values",
        "old_multi_enum_values",
        "type"
    ]

    def __init__(self):
        super().__init__()
        self.session_bookmark = None

    def get_objects(self):
        """Get stream object"""
        bookmark = self.get_bookmark()
        self.session_bookmark = bookmark
        opt_fields = ",".join(self.fields)

        # list of project ids
        project_ids = []

        for workspace in self.call_api("workspaces"):
            for project in self.call_api("projects", workspace=workspace["gid"]):
                project_ids.append(project["gid"])

        project_ids_count = len(project_ids)

        if not project_ids:
            return

        with ThreadPoolExecutor(max_workers=min(32, project_ids_count)) as executor:
            tasks_arguments = [project_id for project_id in project_ids]
            tasks_results = executor.map(self.get_task_gids, tasks_arguments)

            for task_gids in tasks_results:
                if not task_gids:
                    continue

                stories_arguments = [(task_gid, opt_fields) for task_gid in task_gids]
                stories_results = executor.map(self.get_stories, stories_arguments)

                for stories in stories_results:
                    yield from stories

        self.update_bookmark(self.session_bookmark)

    def get_task_gids(self, project_id):
        return [task.get("gid") for task in self.call_api("tasks", project=project_id)]

    def get_stories(self, params):
        task_gid, opt_fields = params
        stories = []

        for story in Context.asana.client.stories.get_stories_for_task(
            task_gid=task_gid,
            opt_fields=opt_fields,
            timeout=self.request_timeout,
        ):
            self.session_bookmark = self.get_updated_session_bookmark(
                self.session_bookmark, story[self.replication_key]
            )
            if self.is_bookmark_old(story[self.replication_key]):
                stories.append(story)

        return stories


Context.stream_objects["stories"] = Stories
