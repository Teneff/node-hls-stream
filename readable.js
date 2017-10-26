const stream = require('stream');
const crypto = require('crypto');
const debug = require('debug');
const fetch = require('node-fetch');
const HLS = require('hls-parser');
const utils = require('./utils');

const print = debug('hls-stream');

const STATE_NO_PLAYLIST = 'no-playlist';
const STATE_PLAYLIST_RETRIEVING = 'no-playlist';
const STATE_MASTER_PLAYLIST_PARSED = 'master-playlist-parsed';
const STATE_MEDIA_PLAYLIST_PARSED = 'media-playlist-parsed';
const STATE_NO_MORE_DATA = 'no-more-data';

function digest(str) {
  const md5 = crypto.createHash('md5');
  md5.update(str, 'utf8');
  return md5.digest('hex');
}

function trimData(data, byterange) {
  if (byterange) {
    const offset = byterange.offset || 0;
    const length = byterange.length || data.length - offset;
    return data.slice(offset, offset + length);
  }
  return data;
}

function getUrl(url, base) {
  return utils.createUrl(url, base).href;
}

class ReadStream extends stream.Readable {
  constructor(url) {
    super({
      objectMode: true
    });
    this.state = STATE_NO_PLAYLIST;
    this.url = url;
    this.masterPlaylist = null;
    this.mediaPlaylists = [];
    this.counter = 0;
  }

  _INCREMENT() {
    this.counter++;
  }

  _DECREMENT() {
    this.counter--;
  }

  get exhausted() {
    return this.state === STATE_NO_MORE_DATA && this.counter === 0;
  }

  _deferIfUnchanged(url, hash) {
    const mediaPlaylists = this.mediaPlaylists;
    if (mediaPlaylists.length === 0) {
      return false;
    }
    for (const playlist of mediaPlaylists) {
      const waitSeconds = playlist.targetDuration * 1.5;
      if (playlist.playlistType !== 'VOD' && playlist.hash === hash) {
        print(`No update. Wait for a period of one-half the target duration before retrying (${waitSeconds}) sec`);
        setTimeout(() => {
          this._loadPlaylist(url);
        }, waitSeconds * 1000);
        return true;
      }
    }
    return false;
  }

  _updateMasterPlaylist(playlist) {
    this.masterPlaylist = playlist;
    this.mediaPlaylists = [];
    this.updateVariant();
  }

  updateVariant() {
    if (this.exhausted) {
      utils.THROW(new Error('the stream has already been exhausted'));
    }
    const playlist = this.masterPlaylist;
    const variants = playlist.variants;
    let currentVariant = 0;
    this._emit('variants', variants, index => {
			// Get feedback from the client synchronously
      currentVariant = index;
    });
    playlist.currentVariant = currentVariant;
    const variant = variants[currentVariant];
    this._loadPlaylist(getUrl(variant.uri, playlist.uri));
    this._updateRendition(playlist, variant);
  }

  _updateRendition(playlist, variant) {
    ['audio', 'video', 'subtitles', 'closedCaptions'].forEach(type => {
      const renditions = variant[type];
      let currentRendition = 0;
      if (renditions.length > 0) {
        this._emit('renditions', renditions, index => {
					// Get feedback from the client synchronously
          currentRendition = index;
        });
        variant.currentRenditions[type] = currentRendition;
        const url = renditions[currentRendition].uri;
        if (url) {
          this._loadPlaylist(getUrl(url, playlist.uri));
        }
      }
    });
  }

  _updateMediaPlaylist(playlist) {
    const mediaPlaylists = this.mediaPlaylists;
    const oldPlaylistIndex = mediaPlaylists.findIndex(elem => {
      if (elem.uri === playlist.uri) {
        return true;
      }
      return false;
    });

    const oldPlaylist = oldPlaylistIndex === -1 ? null : mediaPlaylists[oldPlaylistIndex];
    const newSegments = playlist.segments;
    for (const segment of newSegments) {
      if (oldPlaylist) {
        const oldSegment = oldPlaylist.segments.find(elem => {
          if (elem.uri === segment.uri) {
            return true;
          }
          return false;
        });
        if (oldSegment) {
          segment.data = oldSegment.data;
          segment.key = oldSegment.key;
          segment.map = oldSegment.map;
        } else {
          this._loadSegment(playlist, segment);
        }
      } else {
        this._loadSegment(playlist, segment);
      }
    }

    if (oldPlaylist) {
      mediaPlaylists[oldPlaylistIndex] = playlist;
    } else {
      mediaPlaylists.push(playlist);
    }

    if (playlist.playlistType === 'VOD' || playlist.endlist) {
      this.state = STATE_NO_MORE_DATA;
    } else {
      print(`Wait for at least the target duration before attempting to reload the Playlist file again (${playlist.targetDuration}) sec`);
      setTimeout(() => {
        this._loadPlaylist(playlist.uri);
      }, playlist.targetDuration * 1000);
    }
  }

  _emitPlaylistEvent(playlist) {
    if (!playlist.isMasterPlaylist) {
      return this._emit('data', playlist);
    }
    for (const sessionData of playlist.sessionDataList) {
      if (!sessionData.value && !sessionData.data) {
        return;
      }
    }
    for (const sessionKey of playlist.sessionKeyList) {
      if (!sessionKey.data) {
        return;
      }
    }
    this._emit('data', playlist);
  }

  _loadPlaylist(url) {
    this._INCREMENT();
    fetch(url).then(result => {
      result.text().then(text => {
        this._DECREMENT();
        const hash = digest(text);
        if (this._deferIfUnchanged(url, hash)) {
					// The file is not changed
          return;
        }
        const playlist = HLS.parse(text);
        playlist.source = text;
        playlist.uri = url;
        if (playlist.isMasterPlaylist) {
					// Master Playlist
          this.state = STATE_MASTER_PLAYLIST_PARSED;
          this._emitPlaylistEvent(playlist);
          if (playlist.sessionDataList.length > 0) {
            this._loadSessionData(playlist, () => {
              this._emitPlaylistEvent(playlist);
            });
          }
          if (playlist.sessionKeyList.length > 0) {
            this._loadSessionKey(playlist, () => {
              this._emitPlaylistEvent(playlist);
            });
          }
          this._updateMasterPlaylist(playlist);
        } else {
					// Media Playlist
          this.state = STATE_MEDIA_PLAYLIST_PARSED;
          playlist.hash = hash;
          this._emitPlaylistEvent(playlist);
          this._updateMediaPlaylist(playlist);
        }
      });
    }).catch(err => this._emit('error', err));
  }

  _emitDataEvent(segment) {
    if (!segment.data) {
      return;
    }
    if (segment.key && !segment.key.data) {
      return;
    }
    if (segment.map && !segment.map.data) {
      return;
    }
    this._emit('data', segment);
  }

  _loadSegment(playlist, segment) {
    this._INCREMENT();
    fetch(getUrl(segment.uri, playlist.uri)).then(result => {
      return result.text().then(text => {
        this._DECREMENT();
        segment.data = trimData(text, segment.byterange);
        segment.mimeType = result.headers.get('content-type');
        this._emitDataEvent(segment);
        if (segment.key) {
          this._loadKey(playlist, segment.key, () => {
            this._emitDataEvent(segment);
          });
        }
        if (segment.map) {
          this._loadMap(playlist, segment.map, () => {
            this._emitDataEvent(segment);
          });
        }
      });
    }).catch(err => this._emit('error', err));
  }

  _loadSessionData(playlist, cb) {
    const list = playlist.sessionDataList;
    for (const sessionData of list) {
      if (sessionData.value || !sessionData.url) {
        continue;
      }
      this._INCREMENT();
      fetch(getUrl(sessionData.uri, playlist.uri)).then(result => {
        result.text().then(text => {
          this._DECREMENT();
          sessionData.data = utils.tryCatch(() => {
            return JSON.parse(text);
          }, err => {
            print(`The session data MUST be formatted as JSON. ${err.stack}`);
          });
          cb();
        });
      }).catch(err => this._emit('error', err));
    }
  }

  _loadSessionKey(playlist, cb) {
    const list = playlist.sessionKeyList;
    for (const key of list) {
      this._loadKey(playlist, key, cb);
    }
  }

  _loadKey(playlist, key, cb) {
    this._INCREMENT();
    fetch(getUrl(key.uri, playlist.uri)).then(result => {
      result.text().then(text => {
        this._DECREMENT();
        key.data = text;
        cb();
      });
    }).catch(err => this._emit('error', err));
  }

  _loadMap(playlist, map, cb) {
    this._INCREMENT();
    fetch(getUrl(map.uri, playlist.uri)).then(result => {
      result.text().then(text => {
        this._DECREMENT();
        map.data = trimData(text, map.byterange);
        map.mimeType = result.headers.get('content-type');
        cb();
      });
    }).catch(err => this._emit('error', err));
  }

  _emit(...params) {
    if (params[0] === 'data') {
      this.push(params[1]);
    } else {
      this.emit(...params);
    }
    if (this.state === STATE_NO_MORE_DATA && this.counter === 0) {
      this.push(null);
      this.masterPlaylist = null;
      this.mediaPlaylists = null;
    }
  }

  _read() {
    if (this.state === STATE_NO_PLAYLIST) {
      this._loadPlaylist(this.url);
      this.state = STATE_PLAYLIST_RETRIEVING;
    }
  }
}

module.exports = ReadStream;
