import click
import os
from .commands.backup import Backup
from .commands.restore import Restore
from .commands.upload import Upload
from .commands.report import Report

class Config(object):
    """Configuration class to connect group command and 
    sub commands"""

    def __init__(self):
        self.dbtype = 'mongodb'


pass_config = click.make_pass_decorator(Config, ensure=True)


@click.group()
@click.option('--dbtype', default='mongodb',
              help="Type of database")
@click.option('--home-dir', type=click.Path(),
              help="Home directory location")
@pass_config
def main(config, dbtype, home_dir):
    """Main Command to handle ccsdm commands"""
    config.dbtype = dbtype
    if home_dir is None:
        home_dir = os.path.expanduser('~')
    config.home_dir = home_dir


@main.command()
@click.option('--out', '-o', default=os.path.expanduser('~'),
              help='Specifies the "dump" folder path')
@pass_config
def backup(config, out):
    """Handle all databases' backup"""
    click.echo('Backup command runs')
    bkup = Backup(out)
    bkup.run()
    click.echo('"backup" command finished running')

@main.command()
@click.option('--filepath', '-f', default=os.path.expanduser('~'),
              help='Absolute path of the dump folder to be restored')
@pass_config
def restore(config, filepath):
    """Handle all databases' restoration"""
    click.echo('Restore command runs')
    rstr = Restore(filepath + '\\' + 'dump')
    rstr.run()
    click.echo('"restore" command finished running')


@main.command()
@click.argument('source')
@click.argument('desti')
@click.option('--sheetname', '-s',
              help="Sheet name of the excel sheet to be read")
@pass_config
def upload(config, source, desti, sheetname):
    """Upload Excel files into MongoDB database"""
    upld = Upload(config.home_dir, source, desti, sheetname)
    (is_valid, msg) = upld.validate()
    click.echo(msg)
    if is_valid:
        click.echo('Execute "upload"...')
        upld.read_xl_upload_mongo()

    
@main.command()
@click.option('--large/--no-large', default=False, help="Simple or Complex report")
@click.option('--rsource', '-s', type=click.Choice(['bd', 'sfdc', 'mix']),
              help="Which source to use to pull report")
@click.option('--rtype', '-t',
              type=click.Choice([
                  'generate', 'snapshot', 'whitespace', 
              ]),
              help="Defines type of the reports")
@click.option('--servindi',
              type=click.Choice([
                  'y', 'n', 'Y', 'N', 'YES', 'NO', 'yes', 'no', 'product',
                  'service', 'products', 'services', 'PRODUCT', 'SERVICE',
                  'PRODUCTS', 'SERVICES', 'p', 's', 'P', 'S'
              ]),
              help="Takes services_indicator (Y/N)")
@click.option('--sl3', help="Takes sales_level_3")
@click.option('--sl4', help="Takes sales_level_4")
@click.option('--sl5', help="Takes sales_level_5")
@click.option('--sl6', help="Takes sales_level_6")
@click.option('--sa', help="Takes sales_agent")
@click.option('--year','-y',  help="Takes Financial Year")
@click.option('--quarter','-q',  help="Takes Financial Quarter")
@click.option('--month','-m',  help="Takes Financial Month")
@click.option('--week','-w',  help="Takes Financial Week")
@pass_config
def makereport(config, large, rsource, rtype, servindi, sl3, sl4, sl5, sl6, sa,
           year, quarter, month, week):
    """Generates Reports from Various Sources"""
    rpt = Report(config.home_dir, large, rsource, rtype, servindi, sl3, sl4,
                 sl5, sl6, sa, year, quarter, month, week)
    click.echo(rpt)
    rpt.run()
    click.echo('Report Finished!')

    
