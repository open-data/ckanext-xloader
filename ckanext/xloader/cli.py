# -*- coding: utf-8 -*-

import sys
import click
from ckanext.xloader.command import XloaderCmd
# (canada fork only): add db init
from ckanext.xloader.db import init
# (canada fork only): ckan.plugins.toolkit
from ckan.plugins.toolkit import config, get_action, ObjectNotFound

# Click commands for CKAN 2.9 and above


@click.group(short_help='Perform XLoader related actions')
def xloader():
    """xloader commands
    """
    pass


@xloader.command()
def status():
    """Shows status of jobs
    """
    cmd = XloaderCmd()
    cmd.print_status()


@xloader.command()
# (canada fork only): support resource IDs
# TODO: upstream contrib??
@click.argument(u'dataset-spec', required=False)
@click.option('-y', is_flag=True, default=False, help='Always answer yes to questions')
@click.option('--dry-run', is_flag=True, default=False, help='Don\'t actually submit any resources')
@click.option('--queue', help='Queue name for asynchronous processing, unused if executing immediately')
@click.option('--sync', is_flag=True, default=False,
              help='Execute immediately instead of enqueueing for asynchronous processing')
# (canada fork only): support resource IDs
# TODO: upstream contrib??
@click.option('-r', '--resource-id', type=click.STRING, help='A CKAN Resource ID.', required=False)
def submit(dataset_spec, y, dry_run, queue, sync, resource_id):
    """
        xloader submit [options] <dataset-spec>
    """
    cmd = XloaderCmd(dry_run)

    # (canada fork only): support resource IDs
    # TODO: upstream contrib??
    if resource_id:
        cmd._setup_xloader_logger()
        try:
            res_dict = get_action('resource_show')(
                {'ignore_auth': True}, {'id': resource_id})
            user = get_action('get_site_user')(
                {'ignore_auth': True}, {})
        except ObjectNotFound:
            click.echo('Resource %s not found')
            raise click.Abort()
        cmd._submit_resource(res_dict, user=user, sync=sync, queue=queue)
    elif dataset_spec == 'all':
        cmd._setup_xloader_logger()
        cmd._submit_all(sync=sync, queue=queue)
    elif dataset_spec == 'all-existing':
        _confirm_or_abort(y, dry_run)
        cmd._setup_xloader_logger()
        cmd._submit_all_existing(sync=sync, queue=queue)
    elif dataset_spec:
        pkg_name_or_id = dataset_spec
        cmd._setup_xloader_logger()
        cmd._submit_package(pkg_name_or_id, sync=sync, queue=queue)
    else:
        # (canada fork only): support resource IDs
        # TODO: upstream contrib??
        click.echo('No <dataset-spec> or --resource-id supplied.')
        raise click.Abort()

    if cmd.error_occured:
        print('Finished but saw errors - see above for details')
        sys.exit(1)


# (canada fork only): add db init
@xloader.command()
def db_init():
    """
    Creates Xloader database tables.
    """
    init(config)


def get_commands():
    return [xloader]


def _confirm_or_abort(yes, dry_run):
    if yes or dry_run:
        return
    question = (
        "Data in any datastore resource that isn't in their source files "
        "(e.g. data added using the datastore API) will be permanently "
        "lost. Are you sure you want to proceed?"
    )
    if not click.confirm(question):
        print("Aborting...")
        sys.exit(0)
