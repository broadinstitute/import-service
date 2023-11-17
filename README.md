# import-service
Terra Import Service. Tech doc [here](https://docs.google.com/document/d/1MeL9J5UqhtCg6SLD2Z9S_SsX3L9jYlZnSpfn2HJptc8/edit#).

A walkthrough of the code in this repo is available at [WALKTHROUGH.md](WALKTHROUGH.md).

## Developer notes

### First time setup

Python version 3.9 is required for import-service to run properly.

Create and activate the Python virtualenvironment:

```
$ python3 -m venv venv
$ source venv/bin/activate
(venv) $ poetry install
```

You should periodically run the `poetry install` line within your venv to keep it up-to-date with changes in dependencies.

### Troubleshooting first time setup

If you have problems running `poetry install` for the first time and encounter an error like:

`The license_file parameter is deprecated, use license_files instead.`

There is an incompatibility between pyyaml 5.4.1 and cython 3+ which you can work around with the following from your venv:
```
pip install wheel
pip install "cython<3.0.0" && pip install --no-build-isolation pyyaml==5.4.1
```

([src](https://github.com/yaml/pyyaml/issues/724#issuecomment-1788120324) for workaround)

### Normal usage

Activate and deactivate the venv:
```
$ source venv/bin/activate
<do all your work here>
(venv) $ deactivate
```

To run tests:
```
(venv) poetry run pytest
```

To run the type linter, go to the repo root directory and run:
```
(venv) poetry run mypy ./*.py && poetry run mypy -p app
```

If you'd like to run `import-service` locally, the following steps should help:

```
# At the root of the directory run:
docker build . -t <your-favorite-name>

# Then, find your Image ID -- run the following command and get the SHA value associated with your new container

docker ps && docker run <image-id>
```

You should make mypy happy before opening a PR. Note that errors in some modules will be listed twice. This is annoying, but the good news is that you only have to fix them once.

### Dependency Management

This repo uses `poetry` for dependency management. But, it deploys to App Engine and App Engine requires a `requirements.txt` file. Therefore,
**_you must simultaneously update `requirements.txt` AND poetry's `poetry.lock`/`pyproject.toml` when changing dependencies._**

To sync `requirements.txt` to poetry:
```
poetry export -f requirements.txt -o requirements.txt --without-hashes
```

For other poetry commands, such as to add a new dependency or update an existing dependency, see https://python-poetry.org/docs/cli/.

# Deployment (for Broad only)

Deployments to non-production and production environments are performed in Jenkins.  In order to access Jenkins, you
will need to be on the Broad network or logged on to the Broad VPN.

## Deploy to the "dev" environment

A deployment to `dev` environment will be automatically triggered every time there is a commit or push to the
[develop](https://github.com/broadinstitute/import-service/tree/develop) branch on Github.  If you would like to deploy a different
branch or tag to the `dev` environment, you can do so by following the instructions below, but be aware that a new
deployment of the `develop` branch will be triggered if anyone commits or pushes to that branch.

## Deploy to non-production environments

1. Log in to [Jenkins](https://fc-jenkins.dsp-techops.broadinstitute.org/)
1. Navigate to the [import-service-manual-deploy](https://fc-jenkins.dsp-techops.broadinstitute.org/view/Deploy/job/import-service-manual-deploy/)
   job
1. In the left menu, click [Build with Parameters](https://fc-jenkins.dsp-techops.broadinstitute.org/view/Deploy/job/import-service-manual-deploy/build?delay=0sec)
   and select the `BRANCH_OR_TAG` that you want to deploy, the `TARGET` environment to which you want to deploy, and enter
   the `SLACK_CHANNEL` that you would like to receive notifications of the deploy jobs success/failure
1. Click the `Build` button

## Production Deployment Checklist

When doing a production deployment, each step of the checklist must be performed.

### Production Deployment Preparation

- [ ] Double-check that `requirements.txt` is up to date with poetry; see [Dependency Management](#dependency-management).

- [ ] Create and push a new version tag for the commit you want to deploy; typically this will be the head of the develop branch.
      Go to [Releases](https://github.com/broadinstitute/import-service/releases) and select 'Draft a new Release'.
      [Create a release](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository#creating-a-release)
      with a new tag. Ensure that the tag is incremented properly based on the last released version.

- [ ] Create a ticket for the release and be sure to leave the 'Fix Version' field blank.  Add a checklist to the ticket and select 'Load Templates'
      from the ... menu to the right of the checklist.  Use 'Import Service Release Checklist'.
      You may refer to (or clone) a [previous release ticket](https://broadworkbench.atlassian.net/browse/AJ-1165)
      for an example.  This ticket ensures that the release is recorded for compliance, and that
      any release notes are picked up to be published.  It also helps to keep track of the steps along the way,
      outlined in the next section.

### Deploy and Test
You must deploy to each tier one-by-one and manually test
in each tier after you deploy to it.  This test should consist of uploading a large-ish (~2MB should suffice) tsv to a GCP workspace and ensuring
it asynchronously uploads, as well as any specific changes made in the release.  You may refer also to [this](https://docs.google.com/document/d/17edO6O7Rz5voxWa2oXbTc3pWZArbkOMNJn1woaILlpQ/edit?ts=5e9f6dd5#heading=h.flskep5qnamc)
document, although it is now somewhat out of date.
Your deployment to a tier should not be considered complete until you have
successfully executed each step of the manual test on that tier.  Mark each step complete on the release ticket created above.

To deploy the application code, navigate to the [import-service-manual-deploy](https://fc-jenkins.dsp-techops.broadinstitute.org/view/Deploy/job/import-service-manual-deploy/)
job and click the "Build with Parameters" link.  Select the `TAG` that you just created during the preparation steps and
the `TIER` to which you want to deploy:

- [ ] `dev` deploy job succeeded and manual test passed
      - (Technically, this same commit is probably already running on `dev` courtesy of the automatic `dev` deployment
      job. However, deploying again is an important step because someone else may have triggered a `dev` deployment and
      we want to ensure that you understand the deployment process, the deployment tools are working properly, and that
      everything is working as intended.)
- [ ] `alpha` deploy job succeeded and manual test passed.
- [ ] `staging` deploy job succeeded and manual test passed
- [ ] `prod` deploy job succeeded and manual test passed
      - In order to deploy to `prod`, you must be on the DSP Suitability Roster.  You will need to log into the
      production Jenkins instance and use the "import-service-manual-deploy" job to release the same tag to production.

**NOTE:**
* It is important that you deploy to all tiers.  Because Import Service is an "indie service", we should strive to make sure
that all tiers other than `dev` are kept in sync and are running the same versions of code.  This is essential so that
as other DSP services are tested during their release process, they can ensure that their code will work properly with
the latest version of Bond running in `prod`.

## Deployment Maintenance & Cleanup

`Import Service` is a Google App Engine (GAE) application.  Versions of `import-service` are deployed to GAE either via a merge into the `develop` branch (for `dev` apps),
or via Jenkins for all other environments. [See here for specific details](https://github.com/broadinstitute/import-service#deployment-for-broad-only).
[GAE allows a maximum of 210 versions of any app](https://cloud.google.com/appengine/docs/standard/an-overview-of-app-engine#limits), so we handle cleanup of old apps as
new versions are deployed.

DSP caps the number of versions that can be stored at 50 (this is just an arbitrary number), just to ensure plenty of versions available for rollback if a bug were introduced.

In `cleanup_scripts`:

The `delete-old-app-engine-versions-cleanup` bash script handles cleanup of multiple versions, sorted by oldest. It has a cap of at least 50 versions that must remain after cleanup.

For other environments other than `prod` (such as `alpha` or `staging`), the script must be run manually. This is to ensure that the deletion of versions is intentional by an authenticated user.

For `prod`, the suggestion is that the deletions occur deliberately in the Google App Engine console.