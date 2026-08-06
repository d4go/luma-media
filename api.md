# Luma Media API Design

Base path:

    /api/v1

## Folder

GET /folders

POST /folders

POST /folders/{id}/scan

Example:

``` json
{
  "name":"Movies",
  "path":"/media/movies",
  "type":"movie",
  "scanMode":"watch"
}
```

## Tasks

GET /tasks

GET /tasks/{id}

POST /tasks/{id}/retry

## Media

GET /media

POST /media/{id}/scrape

Options:

``` json
{
 "overwriteNfo":true,
 "overwriteImage":true
}
```

## Settings

GET /settings

PUT /settings

Manage:

-   MetaTube address
-   output format
-   scan interval
-   overwrite policy
