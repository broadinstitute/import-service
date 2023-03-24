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

- [ ] Re-run `poetry export -f requirements.txt -o requirements.txt --without-hashes` locally. Google App Engine requires a
      `requirements.txt` be present in order to run a Python app. Since `poetry` uses `pyproject.toml`, you'll need to convert
      the file to `requirements.txt`

- [ ] Create and push a new [semver](https://semver.org/) tag for the commit you want to deploy; typically this will be
      the head of the develop branch.  You should look at the existing tags
      to ensure that the tag is incremented properly based on the last released version.  Tags should be plain semver numbers
      like `1.0.0` and should not have any additional prefix like `v1.0.0` or `releases/1.0.0`.  Suffixes are permitted so
      long as they conform to the [semver spec](https://semver.org/).

### Deploy and Test
You must deploy to each tier one-by-one and [manually test](https://docs.google.com/document/d/17edO6O7Rz5voxWa2oXbTc3pWZArbkOMNJn1woaILlpQ/edit?ts=5e9f6dd5#heading=h.flskep5qnamc)
in each tier after you deploy to it.  Your deployment to a tier should not be considered complete until you have
successfully executed each step of the [manual test](https://docs.google.com/document/d/17edO6O7Rz5voxWa2oXbTc3pWZArbkOMNJn1woaILlpQ/edit?ts=5e9f6dd5#heading=h.flskep5qnamc)
on that tier.  To deploy the application code, navigate to the [import-service-manual-deploy](https://fc-jenkins.dsp-techops.broadinstitute.org/view/Deploy/job/import-service-manual-deploy/)
job and click the "Build with Parameters" link.  Select the `TAG` that you just created during the preparation steps and
the `TIER` to which you want to deploy:

- [ ] `dev` deploy job succeeded and [manual test](https://docs.google.com/document/d/17edO6O7Rz5voxWa2oXbTc3pWZArbkOMNJn1woaILlpQ/edit?ts=5e9f6dd5#heading=h.flskep5qnamc) passed
      - (Technically, this same commit is probably already running on `dev` courtesy of the automatic `dev` deployment
      job. However, deploying again is an important step because someone else may have triggered a `dev` deployment and
      we want to ensure that you understand the deployment process, the deployment tools are working properly, and that
      everything is working as intended.)
- [ ] `alpha` deploy job succeeded and [manual test](https://docs.google.com/document/d/17edO6O7Rz5voxWa2oXbTc3pWZArbkOMNJn1woaILlpQ/edit?ts=5e9f6dd5#heading=h.flskep5qnamc) passed
- [ ] `staging` deploy job succeeded and [manual test](https://docs.google.com/document/d/17edO6O7Rz5voxWa2oXbTc3pWZArbkOMNJn1woaILlpQ/edit?ts=5e9f6dd5#heading=h.flskep5qnamc) passed
- [ ] `prod` deploy job succeeded and [manual test](https://docs.google.com/document/d/17edO6O7Rz5voxWa2oXbTc3pWZArbkOMNJn1woaILlpQ/edit?ts=5e9f6dd5#heading=h.flskep5qnamc) passed
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

For other environments other than `prod` (such as `alpha, perf, staging`), the script must be run manually. This is to ensure that the deletion of versions is intentional by an authenticated user.

For `prod`, the suggestion is that the deletions occur deliberately in the Google App Engine console.